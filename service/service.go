package service

import (
	"github.com/elvinchan/kvdb"
)

const KVDBServiceName = "KVDB"

type GetRequest struct {
	Key    string       `json:"key"`
	Getter *kvdb.Getter `json:"getter"`
}

type GetResponse struct {
	Node *kvdb.Node `json:"node"`
}

type GetMultiRequest struct {
	Keys   []string     `json:"keys"`
	Getter *kvdb.Getter `json:"getter"`
}

type GetMultiResponse struct {
	NodeMap map[string]kvdb.Node `json:"nodeMap"`
}

type SetRequest struct {
	Key    string       `json:"key"`
	Value  string       `json:"value"`
	Setter *kvdb.Setter `json:"setter"`
}

type SetResponse struct{}

type SetMultiRequest struct {
	KvPairs []string     `json:"kvPairs"`
	Setter  *kvdb.Setter `json:"setter"`
}

type SetMultiResponse struct{}

type DeleteRequest struct {
	Key     string        `json:"key"`
	Deleter *kvdb.Deleter `json:"deleter"`
}

type DeleteResponse struct{}

type DeleteMultiRequest struct {
	Keys    []string      `json:"keys"`
	Deleter *kvdb.Deleter `json:"deleter"`
}

type DeleteMultiResponse struct{}

type ExistRequest struct {
	Key string `json:"key"`
}

type ExistResponse struct {
	Has bool `json:"has"`
}

type CleanupRequest struct{}

type CleanupResponse struct{}

type KVDBInterface interface {
	Get(req GetRequest, resp *GetResponse) error
	GetMulti(req GetMultiRequest, resp *GetMultiResponse) error
	Set(req SetRequest, resp *SetResponse) error
	SetMulti(req SetMultiRequest, resp *SetMultiResponse) error
	Delete(req DeleteRequest, resp *DeleteResponse) error
	DeleteMulti(req DeleteMultiRequest, resp *DeleteMultiResponse) error
	Exist(req ExistRequest, resp *ExistResponse) error
	Cleanup(req CleanupRequest, resp *CleanupResponse) error
}

type KVDBClient struct {
	rpcClient
}

func (c *KVDBClient) Get(key string, opts ...kvdb.GetOption) (*kvdb.Node, error) {
	var gt kvdb.Getter
	for _, opt := range opts {
		opt(&gt)
	}
	req := GetRequest{
		Key:    key,
		Getter: &gt,
	}
	var resp GetResponse
	err := c.doCall(KVDBServiceName+".Get", req, &resp)
	return resp.Node, err
}

func (c *KVDBClient) GetMulti(keys []string, opts ...kvdb.GetOption,
) (map[string]kvdb.Node, error) {
	var gt kvdb.Getter
	for _, opt := range opts {
		opt(&gt)
	}
	req := GetMultiRequest{
		Keys:   keys,
		Getter: &gt,
	}
	var resp GetMultiResponse
	err := c.doCall(KVDBServiceName+".GetMulti", req, &resp)
	return resp.NodeMap, err
}

func (c *KVDBClient) Set(key, value string, opts ...kvdb.SetOption) error {
	var st kvdb.Setter
	for _, opt := range opts {
		opt(&st)
	}
	req := SetRequest{
		Key:    key,
		Value:  value,
		Setter: &st,
	}
	var resp SetResponse
	return c.doCall(KVDBServiceName+".Set", req, &resp)
}

func (c *KVDBClient) SetMulti(kvPairs []string, opts ...kvdb.SetOption) error {
	var st kvdb.Setter
	for _, opt := range opts {
		opt(&st)
	}
	req := SetMultiRequest{
		KvPairs: kvPairs,
		Setter:  &st,
	}
	var resp SetResponse
	return c.doCall(KVDBServiceName+".SetMulti", req, &resp)
}

func (c *KVDBClient) Delete(key string, opts ...kvdb.DeleteOption) error {
	var dt kvdb.Deleter
	for _, opt := range opts {
		opt(&dt)
	}
	req := DeleteRequest{
		Key:     key,
		Deleter: &dt,
	}
	var resp DeleteResponse
	return c.doCall(KVDBServiceName+".Delete", req, &resp)
}

func (c *KVDBClient) DeleteMulti(keys []string, opts ...kvdb.DeleteOption) error {
	var dt kvdb.Deleter
	for _, opt := range opts {
		opt(&dt)
	}
	req := DeleteMultiRequest{
		Keys:    keys,
		Deleter: &dt,
	}
	var resp DeleteMultiResponse
	return c.doCall(KVDBServiceName+".DeleteMulti", req, &resp)
}

func (c *KVDBClient) Exist(key string) (bool, error) {
	req := ExistRequest{
		Key: key,
	}
	var resp ExistResponse
	err := c.doCall(KVDBServiceName+".Exist", req, &resp)
	return resp.Has, err
}

func (c *KVDBClient) Cleanup() error {
	var resp CleanupResponse
	return c.doCall(KVDBServiceName+".Cleanup", CleanupRequest{}, &resp)
}

func (c *KVDBClient) Close() error {
	return c.close()
}
