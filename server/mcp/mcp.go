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
	"github.com/runatlantis/atlantis/server/controllers"
	"github.com/runatlantis/atlantis/server/core/locking"
	"github.com/runatlantis/atlantis/server/events"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
)

// WriteService runs plan/apply commands. It is implemented by
// *controllers.APIController so the MCP server reuses the same machinery as the
// HTTP API.
type WriteService interface {
	RunPlan(request *controllers.APIRequest) (*command.Result, error)
	RunApply(request *controllers.APIRequest) (*command.Result, error)
}

// WriteDeps holds the dependencies for the mutating MCP tools (plan, apply,
// unlock). When nil, only the read-only tools are registered.
type WriteDeps struct {
	Service    WriteService
	DeleteLock events.DeleteLockCommand
}

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
// "Authorization: Bearer <token>" header, except for the /healthz endpoint
// which is always unauthenticated so load balancers can probe it.
// If write is non-nil, the mutating tools (plan, apply, unlock) are also
// registered; otherwise only the read-only tools are exposed.
func NewServer(port int, version, token string, locker locking.Locker, write *WriteDeps, logger logging.SimpleLogging) *Server {
	mcpHandler := bearerAuth(token, mcpserver.NewStreamableHTTPServer(newMCPServer(version, locker, write, logger)))
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthz)
	mux.Handle("/", mcpHandler)
	return &Server{
		logger: logger,
		port:   port,
		httpServer: &http.Server{
			Addr:              fmt.Sprintf(":%d", port),
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
		},
	}
}

// healthz is an unauthenticated liveness endpoint for load balancer probes.
func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
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

func newMCPServer(version string, locker locking.Locker, write *WriteDeps, logger logging.SimpleLogging) *mcpserver.MCPServer {
	s := mcpserver.NewMCPServer(
		"atlantis",
		version,
		mcpserver.WithToolCapabilities(false),
		mcpserver.WithRecovery(),
	)
	registerTools(s, locker, version)
	if write != nil {
		registerWriteTools(s, write, logger)
	}
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

// planApplyArgs are the shared tool arguments for atlantis_plan/atlantis_apply.
func planApplyArgs() []mcp.ToolOption {
	return []mcp.ToolOption{
		mcp.WithString("repo", mcp.Required(), mcp.Description("Repository full name, e.g. owner/repo.")),
		mcp.WithString("ref", mcp.Required(), mcp.Description("Git branch or commit ref to operate on.")),
		mcp.WithString("type", mcp.Description("VCS host type: Github, Gitlab, Gitea, BitbucketCloud, BitbucketServer, AzureDevops. Defaults to Github.")),
		mcp.WithInteger("pr", mcp.Description("Optional pull request number for context and locking.")),
		mcp.WithArray("projects", mcp.Description("Project names to target, as defined in atlantis.yaml."), mcp.WithStringItems()),
		mcp.WithString("dir", mcp.Description("Directory to target (alternative to projects).")),
		mcp.WithString("workspace", mcp.Description("Terraform workspace for 'dir'. Defaults to 'default'.")),
	}
}

func registerWriteTools(s *mcpserver.MCPServer, write *WriteDeps, logger logging.SimpleLogging) {
	s.AddTool(
		mcp.NewTool("atlantis_plan", append([]mcp.ToolOption{
			mcp.WithDescription("Run 'atlantis plan' for a repo/ref and return the terraform plan output per project. Specify projects (by name) and/or a dir."),
			mcp.WithOpenWorldHintAnnotation(true),
		}, planApplyArgs()...)...),
		func(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			apiReq, err := buildAPIRequest(req)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			logger.Info("MCP plan requested: repo=%s ref=%s projects=%v paths=%d", apiReq.Repository, apiReq.Ref, apiReq.Projects, len(apiReq.Paths))
			result, err := write.Service.RunPlan(apiReq)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("plan failed: %s", err)), nil
			}
			return formatResult(result)
		},
	)

	s.AddTool(
		mcp.NewTool("atlantis_apply", append([]mcp.ToolOption{
			mcp.WithDescription("Run 'atlantis apply' for a repo/ref (real terraform apply). Plans first, then applies. NOTE: like the Atlantis API, this does NOT enforce PR-based approval/mergeable requirements."),
			mcp.WithDestructiveHintAnnotation(true),
			mcp.WithOpenWorldHintAnnotation(true),
		}, planApplyArgs()...)...),
		func(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			apiReq, err := buildAPIRequest(req)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			logger.Warn("MCP apply requested: repo=%s ref=%s projects=%v paths=%d", apiReq.Repository, apiReq.Ref, apiReq.Projects, len(apiReq.Paths))
			result, err := write.Service.RunApply(apiReq)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("apply failed: %s", err)), nil
			}
			return formatResult(result)
		},
	)

	s.AddTool(
		mcp.NewTool("atlantis_unlock",
			mcp.WithDescription("Delete an Atlantis project lock (and its saved plan) by lock id."),
			mcp.WithDestructiveHintAnnotation(true),
			mcp.WithIdempotentHintAnnotation(true),
			mcp.WithString("id", mcp.Required(), mcp.Description("The lock id, as returned by atlantis_list_locks.")),
		),
		func(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			id, err := req.RequireString("id")
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			logger.Warn("MCP unlock requested: id=%s", id)
			lock, err := write.DeleteLock.DeleteLock(logger, id)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("unlock failed: %s", err)), nil
			}
			if lock == nil {
				return mcp.NewToolResultError(fmt.Sprintf("no lock found with id %q", id)), nil
			}
			return mcp.NewToolResultText(fmt.Sprintf("unlocked %q (project %q, workspace %q)", id, lock.Project.ProjectName, lock.Workspace)), nil
		},
	)
}

// buildAPIRequest constructs an APIRequest from the tool arguments. At least one
// of "projects" or "dir" must be supplied.
func buildAPIRequest(req mcp.CallToolRequest) (*controllers.APIRequest, error) {
	repo, err := req.RequireString("repo")
	if err != nil {
		return nil, err
	}
	ref, err := req.RequireString("ref")
	if err != nil {
		return nil, err
	}
	apiReq := &controllers.APIRequest{
		Repository: repo,
		Ref:        ref,
		Type:       req.GetString("type", "Github"),
		PR:         req.GetInt("pr", 0),
		Projects:   req.GetStringSlice("projects", nil),
	}
	if dir := req.GetString("dir", ""); dir != "" {
		apiReq.Paths = append(apiReq.Paths, struct {
			Directory string
			Workspace string
		}{Directory: dir, Workspace: req.GetString("workspace", "default")})
	}
	if len(apiReq.Projects) == 0 && len(apiReq.Paths) == 0 {
		return nil, fmt.Errorf("specify at least one of 'projects' or 'dir'")
	}
	return apiReq, nil
}

// projectResultView is the JSON shape returned by the plan/apply tools.
type projectResultView struct {
	Project   string `json:"project,omitempty"`
	Dir       string `json:"dir,omitempty"`
	Workspace string `json:"workspace,omitempty"`
	Status    string `json:"status"`
	Output    string `json:"output,omitempty"`
	Error     string `json:"error,omitempty"`
}

// formatResult turns a command.Result into a tool result, flagging it as an
// error result if any project failed.
func formatResult(result *command.Result) (*mcp.CallToolResult, error) {
	views := make([]projectResultView, 0, len(result.ProjectResults))
	hasErr := false
	for _, pr := range result.ProjectResults {
		v := projectResultView{Project: pr.ProjectName, Dir: pr.RepoRelDir, Workspace: pr.Workspace}
		switch {
		case pr.Error != nil:
			v.Status, v.Error, hasErr = "error", pr.Error.Error(), true
		case pr.Failure != "":
			v.Status, v.Error, hasErr = "failure", pr.Failure, true
		case pr.PlanSuccess != nil:
			v.Status, v.Output = "planned", pr.PlanSuccess.TerraformOutput
		case pr.ApplySuccess != "":
			v.Status, v.Output = "applied", pr.ApplySuccess
		default:
			v.Status = "ok"
		}
		views = append(views, v)
	}
	b, err := json.MarshalIndent(views, "", "  ")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshaling result: %s", err)), nil
	}
	if hasErr {
		return mcp.NewToolResultError(string(b)), nil
	}
	return mcp.NewToolResultText(string(b)), nil
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
