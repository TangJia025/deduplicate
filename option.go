package deduplicate

import "time"

type Option func(*rebloomClient)

func WithTimeout(timeout int) Option {
	return func(cli *rebloomClient) {
		cli.timeout = time.Duration(timeout) * time.Millisecond
	}
}
