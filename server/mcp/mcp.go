// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

// Package mcp provides an experimental Model Context Protocol (MCP) server that
// exposes read-only Atlantis state as tools for AI assistants. It listens on a
// dedicated port and is disabled unless --mcp-enabled is set.
package mcp

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
	"github.com/runatlantis/atlantis/server/core/locking"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
)

// Server wraps an MCP server exposing read-only Atlantis tools over streamable
// HTTP on a dedicated port.
type Server struct {
	httpServer *http.Server
	logger     logging.SimpleLogging
	port       int
}

// lockView is the JSON shape returned by the lock tools.
type lockView struct {
	ID           string    `json:"id"`
	Project      string    `json:"project"`
	Repo         string    `json:"repo"`
	Path         string    `json:"path"`
	Workspace    string    `json:"workspace"`
	PullNum      int       `json:"pull_num"`
	PullURL      string    `json:"pull_url"`
	LockedByUser string    `json:"locked_by_user"`
	Time         time.Time `json:"time"`
}

// NewServer builds an MCP server with the read-only Atlantis tools registered.
// If token is non-empty, every request must carry a matching
// "Authorization: Bearer <token>" header.
func NewServer(port int, version, token string, locker locking.Locker, logger logging.SimpleLogging) *Server {
	var handler http.Handler = mcpserver.NewStreamableHTTPServer(newMCPServer(version, locker))
	handler = bearerAuth(token, handler)
	return &Server{
		logger: logger,
		port:   port,
		httpServer: &http.Server{
			Addr:              fmt.Sprintf(":%d", port),
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
		},
	}
}

// bearerAuth wraps next with a bearer-token check. When token is empty the
// check is skipped and the MCP server runs unauthenticated.
func bearerAuth(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	expected := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("Authorization")
		if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(got)), expected) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func newMCPServer(version string, locker locking.Locker) *mcpserver.MCPServer {
	s := mcpserver.NewMCPServer(
		"atlantis",
		version,
		mcpserver.WithToolCapabilities(false),
		mcpserver.WithRecovery(),
	)
	registerTools(s, locker, version)
	return s
}

func registerTools(s *mcpserver.MCPServer, locker locking.Locker, version string) {
	s.AddTool(
		mcp.NewTool("atlantis_version",
			mcp.WithDescription("Return the running Atlantis server version."),
			mcp.WithReadOnlyHintAnnotation(true),
		),
		func(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText(version), nil
		},
	)

	s.AddTool(
		mcp.NewTool("atlantis_list_locks",
			mcp.WithDescription("List all active Atlantis project locks as JSON."),
			mcp.WithReadOnlyHintAnnotation(true),
		),
		func(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			locks, err := locker.List()
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("listing locks: %s", err)), nil
			}
			views := make([]lockView, 0, len(locks))
			for id, lock := range locks {
				views = append(views, newLockView(id, lock))
			}
			return jsonResult(views)
		},
	)

	s.AddTool(
		mcp.NewTool("atlantis_get_lock",
			mcp.WithDescription("Return details for a single Atlantis lock by its lock id."),
			mcp.WithReadOnlyHintAnnotation(true),
			mcp.WithString("id", mcp.Required(), mcp.Description("The lock id, as returned by atlantis_list_locks.")),
		),
		func(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			id, err := req.RequireString("id")
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			lock, err := locker.GetLock(id)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("getting lock: %s", err)), nil
			}
			if lock == nil {
				return mcp.NewToolResultError(fmt.Sprintf("no lock found with id %q", id)), nil
			}
			return jsonResult(newLockView(id, *lock))
		},
	)
}

func newLockView(id string, lock models.ProjectLock) lockView {
	return lockView{
		ID:           id,
		Project:      lock.Project.ProjectName,
		Repo:         lock.Project.RepoFullName,
		Path:         lock.Project.Path,
		Workspace:    lock.Workspace,
		PullNum:      lock.Pull.Num,
		PullURL:      lock.Pull.URL,
		LockedByUser: lock.User.Username,
		Time:         lock.Time,
	}
}

func jsonResult(v any) (*mcp.CallToolResult, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshaling result: %s", err)), nil
	}
	return mcp.NewToolResultText(string(b)), nil
}

// Start serves the MCP server, blocking until the server is shut down.
func (s *Server) Start() error {
	s.logger.Info("MCP server listening on port %d", s.port)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully stops the MCP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
