# Changelog

## v0.2.0 (2026-10-26)

### Breaking: Sign in with Wakeshift for apps is removed

Every app now identifies its own users, with its own sign-in or with guest sign-in (`GuestAuth`), and acts only in the workspaces it owns. Wakeshift no longer gives apps access to people's own Wakeshift workspaces, so the calls for it are gone. Compared with v0.1.0, these are removed:

- `App.Exchange`, which turned a Wakeshift access token into the agent it signed in.
- `App.Verify` and `Claims`, which checked an access token from Sign in with Wakeshift. `GuestAuth.Verify` is unchanged.
- `App.ResourceMetadata`, `App.Challenge` and `App.RefuseToken`, which pointed MCP clients at Wakeshift to sign in. `GuestAuth` serves its own metadata.
- `App.AuthorizeURL`, `App.SignIn`, `App.Refresh`, `PKCE` and `SignedIn`: Sign in with Wakeshift from your app's own sign-in.
- `App.Redeem`, `Connection`, `App.PublishToWorkspace`, `SourceEvent` and `WorkspaceScope`: publishing into people's own workspaces.
- `Manifest.Accounts`, `Manifest.Workspace` and `AgentScope.RedirectURIs`. Wakeshift refuses a manifest that asks for `accounts: "wakeshift"`, a workspace scope or redirect URIs.
- `Agent.Expires`.

Unchanged: `GuestAuth`, workspaces your app owns and `ActAs`, topics, groups, and events (`DefineEvent`, `Emit`).

### Breaking: `ServeEvents` is now `Attach`

`App.ServeEvents(srv, EventsOptions)` is renamed `App.Attach(srv, AttachOptions)`, the one call that attaches Wakeshift to your MCP server. It returns an `*Attachment`, whose `Wrap(handler)` is the middleware `ServeEvents` returned. `EventsOptions.User` and `EventsOptions.Agent` become one `AttachOptions.Identify(ctx, tokenInfo)`, which returns an `Identity` (`User`, and `Agent` for an agent from `ActAs`); an error counts as no user. It behaves as before:

```go
// before
events := app.ServeEvents(srv, appkit.EventsOptions{User: func(ti *auth.TokenInfo) string { return ti.UserID }})
mux.Handle("/mcp", requireAuth(events(h)))

// now
attached := app.Attach(srv, appkit.AttachOptions{
	Identify: func(_ context.Context, ti *auth.TokenInfo) (appkit.Identity, error) {
		return appkit.Identity{User: ti.UserID}, nil
	},
})
mux.Handle("/mcp", requireAuth(attached.Wrap(h)))
```

## v0.1.0

The first release.
