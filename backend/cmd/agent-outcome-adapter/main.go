// agent-outcome-adapter is a local-only proxy that adds independently verified
// file/SVG receipts to Claude tool results before forwarding them to CC Switch.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/chenyme/grok2api/backend/internal/outcomeadapter"
)

type rootsFlag []string

func (values *rootsFlag) String() string {
	return strings.Join(*values, ",")
}

func (values *rootsFlag) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("allow-root must not be empty")
	}
	*values = append(*values, value)
	return nil
}

func main() {
	var roots rootsFlag
	listenAddress := flag.String("listen", "127.0.0.1:15722", "local listen address")
	upstream := flag.String("upstream", "http://127.0.0.1:15721", "CC Switch upstream URL")
	workingDir := flag.String("working-dir", "", "base directory for relative tool file paths")
	maxRequestBytes := flag.Int64("max-request-bytes", 0, "maximum transformed request bytes (0 uses default)")
	toolCallTTL := flag.Duration("tool-call-ttl", 30*time.Minute, "retention period for emitted tool calls")
	flag.Var(&roots, "allow-root", "allowed local artifact root (repeatable; required for verification)")
	flag.Parse()

	if host, _, err := net.SplitHostPort(*listenAddress); err != nil || !isLoopbackHost(host) {
		log.Fatalf("-listen must be a loopback host: %q", *listenAddress)
	}

	adapter, err := outcomeadapter.NewServer(outcomeadapter.Config{
		UpstreamURL:     *upstream,
		AllowedRoots:    []string(roots),
		WorkingDir:      *workingDir,
		ToolCallTTL:     *toolCallTTL,
		MaxRequestBytes: *maxRequestBytes,
		OnTransform: func() {
			log.Print("verified Anthropic tool result forwarded with strict agent contract")
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	server := &http.Server{
		Addr:              *listenAddress,
		Handler:           adapter,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	shutdownContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-shutdownContext.Done()
		context, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(context); err != nil {
			log.Printf("adapter shutdown: %v", err)
		}
	}()

	log.Printf("agent outcome adapter listening on %s; upstream=%s; allowed_roots=%d", *listenAddress, *upstream, len(roots))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func isLoopbackHost(host string) bool {
	host = strings.TrimSpace(host)
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
