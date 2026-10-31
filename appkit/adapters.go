package appkit

// appkit's side of its adapters for other MCP libraries (appkit/mcpgo): the
// calls an adapter makes to attach Wakeshift to a server the official Go
// SDK doesn't serve, and to run a wrapped tool's call through the same
// scope, rollback and block as Events. They use appkit's own types, so
// they're methods of a value the adapter asserts to its own interface
// (adapt.Impl), not public API.

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/auth"

	"github.com/wakeshift/wakeshift-go/appkit/internal/adapt"
)

func init() { adapt.Impl = bridge{} }

type bridge struct{}

// Attach is Attach without go-sdk: the Attachment (whose Wrap serves MCP
// Events and the wait tool, whatever library serves the rest), and Attach's
// own tools that tools names, for the adapter to add to its server.
func (bridge) Attach(a *App, identify func(context.Context, *auth.TokenInfo) Identity, native func(context.Context, adapt.Request) bool, tools []string) (*Attachment, []adapt.Tool) {
	at := a.attach(identify, native)
	return at, a.eventTools(at, builtinTools(tools))
}

// ArgsOf is a wrapped tool's input's WakeArgs.
func (bridge) ArgsOf(in WakeInput) WakeArgs { return in.wakeArgs() }

// WakeArgsProperties is WakeArgs' properties' schemas, by JSON name, as a
// wrapped tool's input schema lists them.
func (bridge) WakeArgsProperties() map[string]any {
	m, _ := adapt.SchemaMap(argsSchema)
	props, _ := m["properties"].(map[string]any)
	return props
}

// Begin starts a wrapped tool's call, before its handler runs: the
// handler's ToolCall, and ctx with the call's scope (ToolCallFrom).
func (bridge) Begin(ctx context.Context, at *Attachment, r adapt.Request, args WakeArgs) (context.Context, *ToolCall) {
	s := at.newScope(r)
	ctx = context.WithValue(ctx, scopeKey{}, s)
	s.begin(ctx, args)
	return ctx, s.call
}

// End closes the call to further subscribes, once its handler has returned.
func (bridge) End(c *ToolCall) { c.s.end() }

// Finish ends the call once its final result is known (Attachment.finish):
// a failed call is rolled back; otherwise the block to add, if any, or the
// text of the error result that replaces the call's.
func (bridge) Finish(ctx context.Context, c *ToolCall, failed bool) (blk adapt.Block, has bool, failure string) {
	return c.s.at.finish(ctx, c.s, failed)
}

// WarnStructured warns, once per tool, that the call's structured result
// isn't an object, so it can't carry the block.
func (bridge) WarnStructured(c *ToolCall) { c.s.at.warnStructured(c.s) }
