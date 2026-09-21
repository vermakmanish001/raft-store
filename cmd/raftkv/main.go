// Command raftkv runs a raft-store node.
//
// A node serves the key-value API to clients and Raft RPCs to its peers, both
// on one listener. Production would separate them, so peer traffic could sit
// on a private network with its own authentication, but one port keeps a local
// cluster easy to run.
//
// Started with no peers, a node forms a single-member cluster: it elects
// itself immediately and commits on append. The code path is identical to a
// larger cluster, so there is no untested special case for running alone.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/vermakmanish001/raft-store/internal/api"
	"github.com/vermakmanish001/raft-store/internal/config"
	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/replica"
	"github.com/vermakmanish001/raft-store/internal/store"
	"github.com/vermakmanish001/raft-store/internal/transport"
	"github.com/vermakmanish001/raft-store/internal/ui"
	"github.com/vermakmanish001/raft-store/internal/wal"
)

const shutdownTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "raftkv: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		id        = flag.String("id", "node1", "unique ID for this node within the cluster")
		addr      = flag.String("addr", ":8080", "host:port for the client API and peer RPC")
		peerSpec  = flag.String("peers", "", "other nodes, as id=url[,id=url...]")
		advertise = flag.String("advertise", "", "base URL peers and clients use to reach this node")
		dataDir   = flag.String("data-dir", "", "directory for the write-ahead log; empty means state is kept in memory and lost on restart")
		snapEvery = flag.Int("snapshot-threshold", 1024, "compact the log once this many uncompacted entries accumulate; 0 disables compaction")
		logLevel  = flag.String("log-level", "info", "log verbosity: debug, info, warn, error")
		logJSON   = flag.Bool("log-json", false, "emit logs as JSON instead of text")
		withUI    = flag.Bool("ui", true, "serve the bundled cluster dashboard at /")
	)
	flag.Parse()

	logger, err := newLogger(*logLevel, *logJSON)
	if err != nil {
		return err
	}

	peers, err := config.ParsePeers(*peerSpec)
	if err != nil {
		return err
	}
	if _, self := peers[raft.NodeID(*id)]; self {
		return fmt.Errorf("-peers must not list this node's own ID %q", *id)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Bind before announcing anything, so a port collision is reported as an
	// actionable error rather than a node that claims to start and then dies.
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		if errors.Is(err, syscall.EADDRINUSE) {
			return fmt.Errorf("cannot bind %s: another process is already listening there; "+
				"choose a free port with -addr, for example -addr :8081", *addr)
		}
		return fmt.Errorf("cannot bind %s: %w", *addr, err)
	}

	// Client addresses include this node, so a status response names its own
	// address when it is the leader.
	clientAddrs := make(map[raft.NodeID]string, len(peers)+1)
	for peerID, url := range peers {
		clientAddrs[peerID] = url
	}
	clientAddrs[raft.NodeID(*id)] = advertisedURL(*advertise, *addr, listener)

	tr := transport.NewHTTP(raft.NodeID(*id), peers, logger)
	tr.Start()
	defer tr.Close()

	storage, closeStorage, err := openStorage(*dataDir, *id, logger)
	if err != nil {
		return err
	}
	defer closeStorage()

	rep, err := replica.New(replica.Config{
		ID:          raft.NodeID(*id),
		Peers:       config.PeerIDs(peers),
		ClientAddrs: clientAddrs,
		Transport:   tr,
		Store:       store.New(),
		Storage:     storage,
		Logger:      logger,

		SnapshotThreshold: *snapEvery,
	})
	if err != nil {
		return err
	}
	rep.Start()
	defer rep.Close()

	// Peer traffic is routed outside the client API so that heartbeats, which
	// arrive many times a second, do not flood the request log.
	root := http.NewServeMux()
	root.Handle("POST "+transport.Path, tr.Handler())
	root.Handle("/", api.NewServer(rep, logger, api.WithCluster(rep)).Handler())

	// The pattern matches the root path exactly, so everything else still
	// falls through to the API. Registering the dashboard on a bare "/" would
	// shadow the whole API instead.
	if *withUI {
		root.Handle("GET /{$}", ui.Handler())
	}

	srv := &http.Server{
		Handler:           root,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	logger.Info("node listening",
		slog.String("id", *id),
		slog.String("addr", listener.Addr().String()),
		slog.Int("peers", len(peers)),
		slog.String("mode", clusterMode(len(peers))),
		slog.String("storage", storageMode(*dataDir)),
		slog.Bool("dashboard", *withUI),
	)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(listener) }()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil

	case <-ctx.Done():
		logger.Info("shutdown signal received, draining connections",
			slog.Duration("timeout", shutdownTimeout))
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown failed: %w", err)
	}

	logger.Info("shutdown complete")
	return nil
}

// advertisedURL determines the address peers and clients should use to reach
// this node.
//
// An explicit -advertise wins. Otherwise the bound address is used, with a
// wildcard host replaced by loopback: a peer told to connect to "0.0.0.0"
// would dial an address that means "every interface here", not "that machine".
func advertisedURL(advertise, addr string, listener net.Listener) string {
	if advertise != "" {
		return strings.TrimSuffix(advertise, "/")
	}

	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return "http://" + addr
	}
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

// openStorage returns the durable storage for this node, or an in-memory one
// when no data directory is configured.
//
// The file is named after the node so that several nodes can share a directory
// during local testing without one silently appending to another's log, which
// would mix two nodes' terms and votes into nonsense.
func openStorage(dataDir, id string, logger *slog.Logger) (raft.Storage, func(), error) {
	if dataDir == "" {
		logger.Warn("no -data-dir configured: term, vote, and log are kept in memory " +
			"and lost on restart, which is unsafe outside development")
		return raft.NewMemoryStorage(), func() {}, nil
	}

	path := filepath.Join(dataDir, id+".wal")
	w, err := wal.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("opening write-ahead log: %w", err)
	}

	if w.RecoveredPartialRecord() {
		// Expected after a crash, and worth saying out loud: it confirms the
		// previous run ended abruptly rather than shutting down cleanly.
		logger.Info("discarded an incomplete record from the previous run",
			slog.String("path", path))
	}

	return w, func() { w.Close() }, nil
}

func storageMode(dataDir string) string {
	if dataDir == "" {
		return "in-memory"
	}
	return "write-ahead log"
}

func clusterMode(peerCount int) string {
	if peerCount == 0 {
		return "single-node"
	}
	return "cluster"
}

func newLogger(level string, asJSON bool) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid -log-level %q: want debug, info, warn, or error", level)
	}

	opts := &slog.HandlerOptions{Level: lvl}

	var handler slog.Handler = slog.NewTextHandler(os.Stdout, opts)
	if asJSON {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}
	return slog.New(handler), nil
}
