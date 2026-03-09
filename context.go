package winter

import "context"

type ctxKey int

const clientKey ctxKey = iota

// withClient stores the client in ctx so workers can retrieve it with ClientFromContext.
func withClient(ctx context.Context, c *Client) context.Context {
	return context.WithValue(ctx, clientKey, c)
}

// ClientFromContext extracts the Client from a worker's context, allowing
// handlers to enqueue downstream jobs.
func ClientFromContext(ctx context.Context) *Client {
	c, _ := ctx.Value(clientKey).(*Client)
	return c
}
