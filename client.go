package deduplicate

import "context"

// 去重key是否存在的code
const (
	CodeKeyExist = 20000 + iota
	CodeKeyNonExist
)

// Client 对外的接口，包括Check（仅检查）和Write（写入，并检查）两种方法
type Client interface {
	Check(ctx context.Context, keys []*KeyInfo) error
	Write(ctx context.Context, keys []*KeyInfo) error
}

// KeyInfo 去重Key信息
type KeyInfo struct {
	BusId    string // 业务id
	Key      string // 准备去重的key
	Days     int32  // 去重天数
	RouteKey string // 路由key
	Code     int32  // 去重code，请求时不填，返回后根据本字段判断是否重复
}

// NewClient 生成一个去重client
func NewClient(opts ...Option) Client {
	return newRebloomClient(opts...)
}
