package cluster

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/runonflux/flux-drop/internal/analytics"
)

func (n *Node) startAnalytics() {
	archive, err := analytics.NewArchive(filepath.Join(n.config.ContentDir, "analytics"))
	if err != nil {
		return
	} // optional telemetry must never prevent coordinator startup
	n.analytics = archive
	ctx, cancel := context.WithCancel(context.Background())
	n.analyticsCancel = cancel
	n.analyticsDone = make(chan struct{})
	go func() { defer close(n.analyticsDone); archive.Run(ctx) }()
}
func (c *Client) SubmitPageViews(ctx context.Context, snapshot analytics.Snapshot) error {
	if snapshot.Node != c.id {
		return ErrInvalid
	}
	_, err := c.call(ctx, rpcRequest{Method: "analytics_put", Analytics: &snapshot})
	return err
}
func (c *Client) PageViews(ctx context.Context, query analytics.Query) (analytics.Result, error) {
	result, err := c.call(ctx, rpcRequest{Method: "analytics_query", AnalyticsQuery: &query})
	if err != nil {
		return analytics.Result{}, err
	}
	if result.Analytics == nil {
		return analytics.Result{}, analytics.ErrUnavailable
	}
	return *result.Analytics, nil
}
func analyticsError(err error) error {
	switch {
	case errors.Is(err, analytics.ErrInvalid):
		return ErrInvalid
	case errors.Is(err, analytics.ErrBusy):
		return ErrCapacity
	default:
		return err
	}
}
