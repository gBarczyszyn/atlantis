// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/runatlantis/atlantis/server/core/locking"
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

func testClient(t *testing.T, locker locking.Locker) (*client.Client, context.Context) {
	t.Helper()
	c, err := client.NewInProcessClient(newMCPServer("v1.2.3", locker))
	Ok(t, err)
	ctx := context.Background()
	Ok(t, c.Start(ctx))
	_, err = c.Initialize(ctx, mcp.InitializeRequest{})
	Ok(t, err)
	t.Cleanup(func() { c.Close() })
	return c, ctx
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
	srv := NewServer(port, "v9.9.9", &fakeLocker{}, logging.NewNoopLogger(t))

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

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Ok(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
