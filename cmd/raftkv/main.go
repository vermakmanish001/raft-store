// Command raftkv runs a raft-store node.
//
// At Milestone 1 a node is standalone: it serves the key-value API from memory
// with no replication and no durability, so all data is lost on exit. Raft
// joins the picture at Milestone 2.
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
	"syscall"
	"time"

	"github.com/vermakmanish001/raft-store/internal/api"
	"github.com/vermakmanish001/raft-store/internal/store"
)

// shutdownTimeout bounds how long in-flight requests have to finish once a
// termination signal arrives, after which the process exits regardless.
const shutdownTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "raftkv: %v\n", err)
		os.Exit(1)
	}
}

// run holds the real body of main so that every exit path returns an error
// instead of calling os.Exit, which would skip deferred cleanup.
func run() error {
	var (
		addr     = flag.String("addr", ":8080", "host:port for the HTTP API")
		logLevel = flag.String("log-level", "info", "log verbosity: debug, info, warn, error")
		logJSON  = flag.Bool("log-json", false, "emit logs as JSON instead of text")
	)
	flag.Parse()

	logger, err := newLogger(*logLevel, *logJSON)
	if err != nil {
		return err
	}

	// Signals are translated into context cancellation so that shutdown
	// follows the same path as any other cancellation. stop must be called to
	// restore default signal handling, which is what makes a second Ctrl-C
	// kill a process that is wedged mid-shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Handler: api.NewServer(store.New(), logger).Handler(),

		// ReadHeaderTimeout bounds how long a client may take to send its
		// headers. Without it, idle connections that never complete a request
		// accumulate until the process runs out of file descriptors.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	// Bind before starting the serve goroutine, so that a bind failure is
	// returned synchronously and the "listening" line is logged only once the
	// port is actually held. Logging from inside the goroutine would announce
	// success a moment before ListenAndServe could fail, which reads as a node
	// that started and then died for no reason.
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		// A port collision is operator error, not a bug, and it is by far the
		// most common startup failure. Say what to do about it rather than
		// surfacing the raw syscall error.
		if errors.Is(err, syscall.EADDRINUSE) {
			return fmt.Errorf("cannot bind %s: another process is already listening there; "+
				"choose a free port with -addr, for example -addr :8081", *addr)
		}
		return fmt.Errorf("cannot bind %s: %w", *addr, err)
	}

	// Report the resolved address rather than the flag, so that -addr :0 shows
	// the port the kernel actually chose.
	logger.Info("node listening",
		slog.String("addr", listener.Addr().String()),
		slog.String("mode", "single-node"),
	)

	// Serve blocks, so it runs on its own goroutine and reports its outcome
	// back through a buffered channel. The buffer keeps the goroutine from
	// leaking if the shutdown path wins the race and nobody ever reads.
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(listener)
	}()

	select {
	case err := <-serveErr:
		// ErrServerClosed only appears after an explicit Shutdown, which has
		// not happened on this branch, so any error here is a real failure.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil

	case <-ctx.Done():
		logger.Info("shutdown signal received, draining connections",
			slog.Duration("timeout", shutdownTimeout),
		)
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

// newLogger builds the process logger from the parsed flags.
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
