package deduplicate

import "time"

// Option function-option模式，实现配置变更可扩展
type Option func(*rebloomClient)

// WithBusId 指定业务id
func WithBusId(busId string) Option {
	return func(cli *rebloomClient) {
		cli.busId = busId
	}
}

// WithTimeout 指定请求下游超时时间
func WithTimeout(timeout int) Option {
	return func(cli *rebloomClient) {
		cli.timeout = time.Duration(timeout) * time.Millisecond
	}
}
