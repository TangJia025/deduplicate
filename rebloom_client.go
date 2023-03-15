package deduplicate

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
)

const (
	// 执行的命令
	CmdCheck = 1
	CmdWrite = 2

	// 布隆过滤器虚拟分区数目
	PartitionNum = 10000
)

var (
	// 业务信息
	mapBusInfo = make(map[string]*BusInfo)
	mu         sync.RWMutex

	// 去重命令的相关参数
	mapRedisCmdValue = map[int]*cmdValue{
		CmdCheck: {
			cmd:         "bf.mexists",
			existResult: 1,
		},
		CmdWrite: {
			cmd:         "bf.madd",
			existResult: 0,
		},
	}
)

// cmdValue 去重命令参数
type cmdValue struct {
	cmd         string // 去重命令
	existResult int    // 重复时的返回值
}

// BusInfo 业务信息
type BusInfo struct {
	BusId        string //  业务id
	Addr         string // redis实例地址
	UserName     string // 用户名
	Password     string // 密码
	PoolSize     int    // 连接池大小
	MinIdleConns int    // 最小连接数
}

type kv struct {
	key, value string
}

type rebloomClient struct {
	*redis.Client
	timeout time.Duration
	busId   string
}

func init() {
	busInfos := readFromConfigCenter()
	for _, busInfo := range busInfos {
		mu.Lock()
		mapBusInfo[busInfo.BusId] = busInfo
		mu.Unlock()
	}

	go updateBusInfo()
}

func updateBusInfo() {
	for {
		time.Sleep(10 * time.Second)

		busInfos := readFromConfigCenter()
		for _, busInfo := range busInfos {
			mu.Lock()
			mapBusInfo[busInfo.BusId] = busInfo
			mu.Unlock()
		}
	}
}

func readFromConfigCenter() []*BusInfo {
	// 真实逻辑应该从远端配置中心读取配置，这里简单构造一下
	return []*BusInfo{
		{
			BusId:        "100",
			Addr:         "192.168.0.1:6379",
			UserName:     "111",
			Password:     "222",
			PoolSize:     500,
			MinIdleConns: 50,
		},
		{
			BusId:        "101",
			Addr:         "192.168.0.2:6379",
			UserName:     "222",
			Password:     "333",
			PoolSize:     500,
			MinIdleConns: 50,
		},
	}

}

func newRebloomClient(opts ...Option) *rebloomClient {
	rebloomCli := &rebloomClient{}
	for _, opt := range opts {
		opt(rebloomCli)
	}

	busInfo, err := getBusInfo(rebloomCli.busId)
	if err != nil {
		return nil
	}

	opt := &redis.Options{
		Addr:         busInfo.Addr,
		Username:     busInfo.UserName,
		Password:     busInfo.Password,
		DialTimeout:  rebloomCli.timeout,
		ReadTimeout:  rebloomCli.timeout,
		WriteTimeout: rebloomCli.timeout,
		PoolSize:     busInfo.PoolSize,
		MinIdleConns: busInfo.MinIdleConns,
	}
	rebloomCli.Client = redis.NewClient(opt)

	return rebloomCli
}

func getBusInfo(busId string) (*BusInfo, error) {
	mu.RLock()
	busInfo, ok := mapBusInfo[busId]
	mu.Unlock()
	if !ok {
		log.Printf("busId:%s invalid", busId)
		return nil, fmt.Errorf("busId:%s invalid", busId)
	}
	return busInfo, nil
}

func (rc *rebloomClient) Check(ctx context.Context, keys []*KeyInfo) error {
	return rc.parallelExec(ctx, keys, CmdCheck)
}

func (rc *rebloomClient) Write(ctx context.Context, keys []*KeyInfo) error {
	return rc.parallelExec(ctx, keys, CmdWrite)
}

func (rc *rebloomClient) parallelExec(ctx context.Context, keys []*KeyInfo, cmd int) error {
	s := time.Now()
	var err error

	defer func() {
		cost := time.Since(s)
		log.Printf("request cost:%vms", cost.Milliseconds())
		if err == nil {
			log.Printf("request succ")
		} else {
			log.Printf("request fail")
		}
	}()

	if err = checkKeys(keys); err != nil {
		return err
	}

	var (
		wg      sync.WaitGroup
		errRsps = make([]error, len(keys))
	)
	for i := range keys {
		wg.Add(1)
		go func(i int) {
			defer func() {
				if e := recover(); e != nil {
					log.Printf("goroutine recover")
				}
			}()

			errRsps[i] = rc.exec(ctx, keys[i], cmd)
		}(i)
	}
	wg.Done()

	for _, e := range errRsps {
		if e != nil {
			err = e
			return err
		}
	}

	return nil
}

func checkKeys(keys []*KeyInfo) error {
	if len(keys) == 0 {
		return errors.New("keys empty")
	}
	for _, key := range keys {
		if _, err := getBusInfo(key.BusId); err != nil {
			return err
		}
		if key.Days < 1 {
			return errors.New("deduplicate days invalid")
		}
		// 其他合法性校验
	}
	return nil
}

func (rc *rebloomClient) exec(ctx context.Context, key *KeyInfo, cmd int) error {
	redisCmd, existResult := mapRedisCmdValue[cmd].cmd, mapRedisCmdValue[cmd].existResult
	kvs := genKVs(key)
	pipe := rc.Client.Pipeline()
	for _, kv := range kvs {
		pipe.Do(ctx, redisCmd, kv.key, kv.value)
	}
	results, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("redis pipe exec err:%v", err)
		return err
	}
	key.Code = CodeKeyNonExist
	for _, result := range results {
		intRes, ok := result.(*redis.IntCmd)
		if !ok {
			log.Printf("redis pipe result:%v invalid", result)
			return fmt.Errorf("redis pipe result:%v invalid", result)
		}
		if int(intRes.Val()) == existResult {
			key.Code = CodeKeyExist
			break
		}
	}
	return nil
}

func genKVs(key *KeyInfo) []kv {
	kvs := make([]kv, 0, key.Days)
	for i := 0; i < int(key.Days); i++ {
		kvs = append(kvs, kv{
			key:   generateKey(key.BusId, i, key.RouteKey),
			value: key.Key,
		})
	}
	return kvs
}

func generateKey(busId string, i int, routeKey string) string {
	date := time.Now().AddDate(0, 0, -i).Format("20060102")
	partition := crc32.ChecksumIEEE([]byte(routeKey)) % PartitionNum
	return fmt.Sprintf("%s_%s_%d", busId, date, partition)
}
