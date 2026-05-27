// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/runatlantis/atlantis/server/controllers"
	"github.com/runatlantis/atlantis/server/core/locking"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
	. "github.com/runatlantis/atlantis/testing"
)

type fakeLocker struct {
	locks map[string]models.ProjectLock
	err   error
}

func (f *fakeLocker) List() (map[string]models.ProjectLock, error) { return f.locks, f.err }

func (f *fakeLocker) GetLock(key string) (*models.ProjectLock, error) {
	if f.err != nil {
		return nil, f.err
	}
	l, ok := f.locks[key]
	if !ok {
		return nil, nil
	}
	return &l, nil
}

func (f *fakeLocker) TryLock(models.Project, string, models.PullRequest, models.User) (locking.TryLockResponse, error) {
	return locking.TryLockResponse{}, nil
}
func (f *fakeLocker) Unlock(string) (*models.ProjectLock, error)             { return nil, nil }
func (f *fakeLocker) UnlockByPull(string, int) ([]models.ProjectLock, error) { return nil, nil }

type fakeWriteService struct {
	planReq  *controllers.APIRequest
	applyReq *controllers.APIRequest
	result   *command.Result
	err      error
}

func (f *fakeWriteService) RunPlan(r *controllers.APIRequest) (*command.Result, error) {
	f.planReq = r
	return f.result, f.err
}

func (f *fakeWriteService) RunApply(r *controllers.APIRequest) (*command.Result, error) {
	f.applyReq = r
	return f.result, f.err
}

type fakeDeleteLock struct {
	gotID string
	lock  *models.ProjectLock
	err   error
}

func (f *fakeDeleteLock) DeleteLock(_ logging.SimpleLogging, id string) (*models.ProjectLock, error) {
	f.gotID = id
	return f.lock, f.err
}

func (f *fakeDeleteLock) DeleteLocksByPull(logging.SimpleLogging, string, int) (int, error) {
	return 0, nil
}

func testClient(t *testing.T, locker locking.Locker) (*client.Client, context.Context) {
	return testClientWrite(t, locker, nil)
}

func testClientWrite(t *testing.T, locker locking.Locker, write *WriteDeps) (*client.Client, context.Context) {
	t.Helper()
	c, err := client.NewInProcessClient(newMCPServer("v1.2.3", locker, write, logging.NewNoopLogger(t)))
	Ok(t, err)
	ctx := context.Background()
	Ok(t, c.Start(ctx))
	_, err = c.Initialize(ctx, mcp.InitializeRequest{})
	Ok(t, err)
	t.Cleanup(func() { c.Close() })
	return c, ctx
}

func writeDeps() (*fakeWriteService, *fakeDeleteLock, *WriteDeps) {
	svc := &fakeWriteService{result: &command.Result{ProjectResults: []command.ProjectResult{
		{
			ProjectName:          "proj1",
			Workspace:            "default",
			ProjectCommandOutput: command.ProjectCommandOutput{PlanSuccess: &models.PlanSuccess{TerraformOutput: "1 to add"}},
		},
	}}}
	del := &fakeDeleteLock{lock: &models.ProjectLock{Workspace: "default"}}
	return svc, del, &WriteDeps{Service: svc, DeleteLock: del}
}

func TestWriteToolsDisabled(t *testing.T) {
	c, ctx := testClient(t, &fakeLocker{}) // write deps nil
	req := mcp.CallToolRequest{}
	req.Params.Name = "atlantis_plan"
	req.Params.Arguments = map[string]any{"repo": "o/r", "ref": "main", "projects": []any{"p"}}
	res, err := c.CallTool(ctx, req)
	Assert(t, err != nil || res.IsError, "plan tool must be unavailable when write is disabled")
}

func TestPlanTool(t *testing.T) {
	svc, _, deps := writeDeps()
	c, ctx := testClientWrite(t, &fakeLocker{}, deps)
	req := mcp.CallToolRequest{}
	req.Params.Name = "atlantis_plan"
	req.Params.Arguments = map[string]any{"repo": "owner/repo", "ref": "main", "projects": []any{"proj1"}}
	res, err := c.CallTool(ctx, req)
	Ok(t, err)
	Assert(t, !res.IsError, "unexpected error result")
	Equals(t, "owner/repo", svc.planReq.Repository)
	Equals(t, "main", svc.planReq.Ref)
	Equals(t, "Github", svc.planReq.Type) // defaulted
	Equals(t, []string{"proj1"}, svc.planReq.Projects)
	tc, ok := res.Content[0].(mcp.TextContent)
	Assert(t, ok, "expected text content")
	Assert(t, strings.Contains(tc.Text, "planned"), "expected planned status")
	Assert(t, strings.Contains(tc.Text, "1 to add"), "expected terraform output")
}

func TestApplyTool(t *testing.T) {
	svc, _, deps := writeDeps()
	c, ctx := testClientWrite(t, &fakeLocker{}, deps)
	req := mcp.CallToolRequest{}
	req.Params.Name = "atlantis_apply"
	req.Params.Arguments = map[string]any{"repo": "owner/repo", "ref": "main", "type": "Gitlab", "dir": "envs/prod", "pr": float64(7)}
	res, err := c.CallTool(ctx, req)
	Ok(t, err)
	Assert(t, !res.IsError, "unexpected error result")
	Equals(t, "Gitlab", svc.applyReq.Type)
	Equals(t, 7, svc.applyReq.PR)
	Equals(t, 1, len(svc.applyReq.Paths))
	Equals(t, "envs/prod", svc.applyReq.Paths[0].Directory)
	Equals(t, "default", svc.applyReq.Paths[0].Workspace)
}

func TestPlanToolRequiresTarget(t *testing.T) {
	_, _, deps := writeDeps()
	c, ctx := testClientWrite(t, &fakeLocker{}, deps)
	req := mcp.CallToolRequest{}
	req.Params.Name = "atlantis_plan"
	req.Params.Arguments = map[string]any{"repo": "owner/repo", "ref": "main"}
	res, err := c.CallTool(ctx, req)
	Ok(t, err)
	Assert(t, res.IsError, "expected error when neither projects nor dir given")
}

func TestUnlockTool(t *testing.T) {
	_, del, deps := writeDeps()
	c, ctx := testClientWrite(t, &fakeLocker{}, deps)
	req := mcp.CallToolRequest{}
	req.Params.Name = "atlantis_unlock"
	req.Params.Arguments = map[string]any{"id": "lock-123"}
	res, err := c.CallTool(ctx, req)
	Ok(t, err)
	Assert(t, !res.IsError, "unexpected error result")
	Equals(t, "lock-123", del.gotID)
}

func callText(t *testing.T, c *client.Client, ctx context.Context, name string, args map[string]any) (*mcp.CallToolResult, string) {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Name = name
	req.Params.Arguments = args
	res, err := c.CallTool(ctx, req)
	Ok(t, err)
	Assert(t, len(res.Content) == 1, "expected exactly one content block, got %d", len(res.Content))
	tc, ok := res.Content[0].(mcp.TextContent)
	Assert(t, ok, "expected text content")
	return res, tc.Text
}

func sampleLock() models.ProjectLock {
	return models.ProjectLock{
		Project:   models.Project{ProjectName: "proj", RepoFullName: "org/repo", Path: "dir"},
		Pull:      models.PullRequest{Num: 7, URL: "https://example.com/pull/7"},
		User:      models.User{Username: "alice"},
		Workspace: "default",
		Time:      time.Now().UTC(),
	}
}

func TestVersionTool(t *testing.T) {
	c, ctx := testClient(t, &fakeLocker{})
	res, text := callText(t, c, ctx, "atlantis_version", nil)
	Assert(t, !res.IsError, "unexpected error result")
	Equals(t, "v1.2.3", text)
}

func TestListLocksTool(t *testing.T) {
	locker := &fakeLocker{locks: map[string]models.ProjectLock{"org/repo/dir/default": sampleLock()}}
	c, ctx := testClient(t, locker)
	res, text := callText(t, c, ctx, "atlantis_list_locks", nil)
	Assert(t, !res.IsError, "unexpected error result")

	var views []lockView
	Ok(t, json.Unmarshal([]byte(text), &views))
	Equals(t, 1, len(views))
	Equals(t, "org/repo/dir/default", views[0].ID)
	Equals(t, "org/repo", views[0].Repo)
	Equals(t, 7, views[0].PullNum)
	Equals(t, "alice", views[0].LockedByUser)
}

func TestListLocksToolError(t *testing.T) {
	c, ctx := testClient(t, &fakeLocker{err: errors.New("boom")})
	res, _ := callText(t, c, ctx, "atlantis_list_locks", nil)
	Assert(t, res.IsError, "expected error result when locker fails")
}

func TestGetLockTool(t *testing.T) {
	locker := &fakeLocker{locks: map[string]models.ProjectLock{"org/repo/dir/default": sampleLock()}}
	c, ctx := testClient(t, locker)

	res, text := callText(t, c, ctx, "atlantis_get_lock", map[string]any{"id": "org/repo/dir/default"})
	Assert(t, !res.IsError, "unexpected error result")
	var view lockView
	Ok(t, json.Unmarshal([]byte(text), &view))
	Equals(t, "proj", view.Project)
	Equals(t, "default", view.Workspace)
}

func TestGetLockToolNotFound(t *testing.T) {
	c, ctx := testClient(t, &fakeLocker{locks: map[string]models.ProjectLock{}})
	res, _ := callText(t, c, ctx, "atlantis_get_lock", map[string]any{"id": "missing"})
	Assert(t, res.IsError, "expected error result for missing lock")
}

func TestGetLockToolMissingArg(t *testing.T) {
	c, ctx := testClient(t, &fakeLocker{})
	res, _ := callText(t, c, ctx, "atlantis_get_lock", nil)
	Assert(t, res.IsError, "expected error result when id is missing")
}

// TestServerHTTP exercises the real streamable HTTP transport, covering
// Start and Shutdown end to end.
func TestServerHTTP(t *testing.T) {
	port := freePort(t)
	srv := NewServer(port, "v9.9.9", "", &fakeLocker{}, nil, logging.NewNoopLogger(t))

	go func() { _ = srv.Start() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	c, err := client.NewStreamableHttpClient(fmt.Sprintf("http://127.0.0.1:%d/mcp", port))
	Ok(t, err)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	Ok(t, c.Start(ctx))

	// Retry initialize until the listener is accepting connections.
	var initErr error
	for i := 0; i < 50; i++ {
		_, initErr = c.Initialize(ctx, mcp.InitializeRequest{})
		if initErr == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	Ok(t, initErr)

	req := mcp.CallToolRequest{}
	req.Params.Name = "atlantis_version"
	res, err := c.CallTool(ctx, req)
	Ok(t, err)
	Assert(t, !res.IsError, "unexpected error result")
	tc, ok := res.Content[0].(mcp.TextContent)
	Assert(t, ok, "expected text content")
	Equals(t, "v9.9.9", tc.Text)
}

// TestHealthz verifies /healthz is reachable without a token even when auth
// is enabled, so load balancers can probe it.
func TestHealthz(t *testing.T) {
	port := freePort(t)
	srv := NewServer(port, "v9.9.9", "s3cret", &fakeLocker{}, nil, logging.NewNoopLogger(t))
	go func() { _ = srv.Start() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	waitListening(t, addr)

	resp, err := http.Get("http://" + addr + "/healthz")
	Ok(t, err)
	defer resp.Body.Close()
	Equals(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	Ok(t, err)
	Equals(t, `{"status":"ok"}`, string(body))
}

// TestServerHTTPAuth verifies the bearer token is enforced over the real
// streamable HTTP transport.
func TestServerHTTPAuth(t *testing.T) {
	port := freePort(t)
	srv := NewServer(port, "v9.9.9", "s3cret", &fakeLocker{}, nil, logging.NewNoopLogger(t))
	go func() { _ = srv.Start() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	waitListening(t, addr)

	url := "http://" + addr + "/mcp"
	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}`

	doPost := func(auth string) int {
		req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
		Ok(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		Ok(t, err)
		defer resp.Body.Close()
		return resp.StatusCode
	}

	Equals(t, http.StatusUnauthorized, doPost(""))
	Equals(t, http.StatusUnauthorized, doPost("Bearer wrong"))
	Assert(t, doPost("Bearer s3cret") != http.StatusUnauthorized, "valid token must not be rejected")
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Ok(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitListening(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server never started listening on %s", addr)
}

func bearerAuthStatus(t *testing.T, token, header string) (int, bool) {
	t.Helper()
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	if header != "" {
		req.Header.Set("Authorization", header)
	}
	rec := httptest.NewRecorder()
	bearerAuth(token, next).ServeHTTP(rec, req)
	return rec.Code, called
}

func TestBearerAuthDisabledWhenEmpty(t *testing.T) {
	code, called := bearerAuthStatus(t, "", "")
	Equals(t, http.StatusOK, code)
	Assert(t, called, "next handler should be called when no token is configured")
}

func TestBearerAuthRejectsMissingHeader(t *testing.T) {
	code, called := bearerAuthStatus(t, "s3cret", "")
	Equals(t, http.StatusUnauthorized, code)
	Assert(t, !called, "next handler must not be called without a valid token")
}

func TestBearerAuthRejectsWrongToken(t *testing.T) {
	code, called := bearerAuthStatus(t, "s3cret", "Bearer nope")
	Equals(t, http.StatusUnauthorized, code)
	Assert(t, !called, "next handler must not be called with a wrong token")
}

func TestBearerAuthAcceptsValidToken(t *testing.T) {
	code, called := bearerAuthStatus(t, "s3cret", "Bearer s3cret")
	Equals(t, http.StatusOK, code)
	Assert(t, called, "next handler should be called with a valid token")
}
