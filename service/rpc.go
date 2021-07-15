package service

import (
	"context"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
	"time"

	"github.com/elvinchan/kvdb"
	"github.com/elvinchan/util-collects/retry"
)

type rpcClient struct {
	*rpc.Client
	Dial func() (*rpc.Client, error)
	mu   sync.RWMutex
}

func DialKVDBService(network, address string) (kvdb.KVDB, error) {
	dialer := func() (*rpc.Client, error) {
		conn, err := net.Dial(network, address)
		if err != nil {
			return nil, err
		}
		return rpc.NewClientWithCodec(jsonrpc.NewClientCodec(conn)), nil
	}
	c, err := dialer()
	if err != nil {
		return nil, err
	}
	return &KVDBClient{
		rpcClient{
			Client: c,
			Dial:   dialer,
		},
	}, nil
}

func (c *rpcClient) close() error {
	return c.Client.Close()
}

func (c *rpcClient) doCall(serviceMethod string, args interface{}, reply interface{}) error {
	return retry.Do(context.Background(), func(ctx context.Context, attempt uint) error {
		if attempt > 0 {
			client, err := c.Dial()
			if err != nil {
				return err
			}
			func() {
				c.mu.Lock()
				defer c.mu.Unlock()
				_ = c.Client.Close()
				c.Client = client
			}()
		}
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.Client.Call(serviceMethod, args, reply)
	}, retry.Backoff(retry.Linear(time.Millisecond*200)), retry.Limit(2))
}
