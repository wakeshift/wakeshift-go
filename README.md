# Wakeshift SDK for Go

Build apps that wake AI agents. Your app is an MCP server that offers events; agents (Claude Code, Codex, ChatGPT, Cursor and others) subscribe to them, and when your app emits one, [Wakeshift](https://wakeshift.ai) wakes every subscribed conversation, in whatever harness it runs.

Go 1.27 or later.

```sh
go get github.com/wakeshift/wakeshift-go@latest
```

```go
import "github.com/wakeshift/wakeshift-go/appkit"
```

## Get a key

In the [Wakeshift console](https://app.wakeshift.ai), open **Build → New app**. It gives you a key, `wst_…`: your app's private key. Keep it secret; Wakeshift keeps only the public half.

```go
app, err := appkit.Open(ctx, os.Getenv("WAKESHIFT_KEY"), "", "https://your-app.example/mcp")
```

An empty address reads `WAKESHIFT_CORE`, then defaults to `https://app.wakeshift.ai`. `Open` asks Wakeshift once which app the key is (`app.ID`). Older `wst_test_<app id>_…` keys still work; they need `WAKESHIFT_CORE`.

## Events

Define each event your app offers once, at startup: its name, the arguments a subscriber picks and what each occurrence carries (both Go types; their JSON Schemas come from them), who may subscribe, and the line it reads as.

```go
type channelArgs struct {
	Channel string `json:"channel" jsonschema:"The channel to watch"`
}
type message struct {
	From string `json:"from"`
	Text string `json:"text"`
}

msg, err := appkit.DefineEvent(app, "chat.message", appkit.EventDef[channelArgs, message]{
	Description: "A new message in a channel",
	Authorize:   func(ctx context.Context, s appkit.Subscriber, a channelArgs) bool { return canRead(s.User, a.Channel) },
	Text:        func(m message, a channelArgs) string { return m.From + " in #" + a.Channel + ": " + m.Text },
})
```

Publish your manifest (it carries the events), attach Wakeshift to your MCP server, and put it in front of the endpoint, after its authentication:

```go
app.PublishManifest(ctx, appkit.Manifest{Description: "Team chat"})
attached := app.Attach(srv, appkit.AttachOptions{
	Identify: func(_ context.Context, ti *auth.TokenInfo) (appkit.Identity, error) {
		return appkit.Identity{User: ti.UserID}, nil
	},
})
mux.Handle("/mcp", requireAuth(attached.Wrap(mcpHandler)))
```

Every client can now subscribe: ChatGPT with MCP Events, every other agent with the `subscribe_events` tool that `Attach` adds, and anyone can see and stop theirs with `list_subscriptions` and `unsubscribe_events`. When something happens, emit it:

```go
msg.Emit(ctx, channelArgs{Channel: "eng"}, message{From: "ada", Text: "shipped it"})
```

Every conversation subscribed to `#eng` wakes with it, framed as information from outside. A **personal** event (`Personal: true`) belongs to each subscriber: `Emit(…, appkit.To(user))` reaches that user's subscriptions only, so it can carry what only they may see ("it's your move: your cards are…"). A tool of your own can subscribe its caller in one step with `msg.Subscribe(ctx, in.Args.Call(req, user), args)`.

## Topics

Events are built on topics, which you can use directly: for publish URLs, waking one subscription with `Send`, or a subscribe tool of your own with `Once` or `Replay`. A tool subscribes the conversation that called it to a topic. Add `appkit.Args` to the tool's arguments: they carry what Wakeshift needs to tell which conversation is calling (`session_key`, `harness`, `conversation_id`).

```go
type watchArgs struct {
	Repo string `json:"repo"`
	appkit.Args
}

mcp.AddTool(srv, &mcp.Tool{Name: "watch_ci", Description: "Wake me when CI finishes on a repo."},
	func(ctx context.Context, req *mcp.CallToolRequest, in watchArgs) (*mcp.CallToolResult, any, error) {
		// Your own access check first: can this user see in.Repo?
		sub, err := app.Subscribe(ctx, in.Args.Call(req, userID), "ci/"+in.Repo, appkit.SubscribeOptions{Replay: 5 * time.Minute})
		if err != nil {
			return nil, nil, err
		}
		// The result carries what this conversation's harness needs to be woken.
		return sub.Addition.Apply(&mcp.CallToolResult{}, "Watching CI on "+in.Repo+"."), nil, nil
	})
```

Then, from anywhere in your app, for example a webhook handler:

```go
app.Publish(ctx, []string{"ci/" + repo}, appkit.TopicEvent{
	ID:   deliveryID, // retries publish once
	Text: "CI failed on main: tests",
	Data: map[string]any{"branch": "main", "url": runURL},
})
```

Every subscribed conversation wakes with the event, framed as information from outside, not as its user's instructions. Topics under `u/<user>/` are private to that user; others are shared, so check access before subscribing. `Send` wakes one subscription, `PublishURL` makes a URL that publishes whatever is posted to it, and `Unsubscribe` and `UnsubscribeUser` end subscriptions.

## Sign-in

Your app identifies its own users, with its own sign-in; Wakeshift never signs them in for you. Pass your user's ID as `user` in `Args.Call`, or from `AttachOptions.Identify`, and Wakeshift keeps a record per user for your app. Users never see Wakeshift. To act for a user's agent, `Workspace` and `AddAgent` set it up, `ActAs` acts for it, and `HarnessAgent` names the agent after its MCP client (`claude-code`). `RequireBearer` checks the tokens your sign-in issues on your MCP endpoint, and `ForgetUser` deletes what Wakeshift keeps for a user.

## Examples

[wakeshift/apps](https://github.com/wakeshift/apps) has complete apps built on this SDK: multiplayer games for agents (Secret Agent, Nightfall), a chat app (Agent Chat) and a Slack bridge (Slackline).

The same calls are in the [TypeScript](https://github.com/wakeshift/wakeshift-node) and [Python](https://github.com/wakeshift/wakeshift-python) SDKs. What changed between releases is in [CHANGELOG](CHANGELOG.md).

## Documentation

The guides and the full reference are at [wakeshift.ai/docs](https://wakeshift.ai/docs): the [quickstart](https://wakeshift.ai/docs/quickstart), [events](https://wakeshift.ai/docs/events), [your users](https://wakeshift.ai/docs/users) and [the Go SDK](https://wakeshift.ai/docs/go). The package's reference is on [pkg.go.dev](https://pkg.go.dev/github.com/wakeshift/wakeshift-go/appkit).

## Testing

```sh
go test ./...
```

The SDK's end-to-end tests run against the Wakeshift server, so they run in Wakeshift's own repository.

## Contributing and security

This repository is generated from Wakeshift's main repository. Pull requests are welcome: we apply them there and publish the result. Each commit here says which commit it came from (`.source-commit`); see [CONTRIBUTING](CONTRIBUTING.md). Report a security problem privately to support@wakeshift.ai, not in an issue ([SECURITY](SECURITY.md)).

## License

Apache License 2.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE).
