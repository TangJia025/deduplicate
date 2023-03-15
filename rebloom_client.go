package deduplicate

import (
	"context"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type rebloomClient struct {
	*redis.Client
	timeout time.Duration
}

func newRebloomClient(opts ...Option) *rebloomClient {
	rebloomCli := &rebloomClient{}
	for _, opt := range opts {
		opt(rebloomCli)
	}
	opt := &redis.Options{
		Addr:         "",
		Username:     "",
		Password:     "",
		DialTimeout:  0,
		ReadTimeout:  0,
		WriteTimeout: 0,
		PoolFIFO:     false,
		PoolSize:     0,
		PoolTimeout:  0,
		MinIdleConns: 0,
		MaxIdleConns: 0,
	}
	rebloomCli.Client = redis.NewClient(opt)
	return rebloomCli
}

func (rc *rebloomClient) Check(ctx context.Context, keys []*KeyInfo) error {
	return nil
}

func (rc *rebloomClient) Write(ctx context.Context, keys []*KeyInfo) error {
	return nil
}
