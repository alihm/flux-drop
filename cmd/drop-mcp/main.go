package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/runonflux/flux-drop/internal/agentmcp"
)

func main() {
	log.SetOutput(os.Stderr) // stdout is reserved for MCP protocol messages.
	publisher, err := agentmcp.New(agentmcp.Config{
		Origin:  os.Getenv("DROP_MCP_ORIGIN"),
		KeyFile: os.Getenv("DROP_MCP_API_KEY_FILE"),
		Root:    os.Getenv("DROP_MCP_ROOT"),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := publisher.Server().Run(ctx, &mcp.StdioTransport{}); err != nil {
		log.Fatal(err)
	}
}
