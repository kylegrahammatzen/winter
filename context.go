package winter

import "context"

type ctxKey int

const clientKey ctxKey = iota

func withClient(ctx context.Context, c *Client) context.Context {
	return context.WithValue(ctx, clientKey, c)
}

func ClientFromContext(ctx context.Context) *Client {
	c, _ := ctx.Value(clientKey).(*Client)
	return c
}
