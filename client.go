package deduplicate

import "context"

const (
	CodeKeyExist = 20000 + iota
	CodeKeyNonExist
)

type Client interface {
	Check(ctx context.Context, keys []*KeyInfo) error
	Write(ctx context.Context, keys []*KeyInfo) error
}

type KeyInfo struct {
	Key      string
	Days     int32
	RouteKey string
	Code     int32
}

func NewClient(opts ...Option) Client {
	return newRebloomClient(opts...)
}
