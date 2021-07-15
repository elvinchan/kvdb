package server

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	"github.com/elvinchan/kvdb"
	"github.com/elvinchan/kvdb/service"
)

type KVServer struct {
	db kvdb.KVDB
}

func StartServer(db kvdb.KVDB, network, address string) error {
	server := rpc.NewServer()
	if err := server.RegisterName(service.KVDBServiceName, &KVServer{db}); err != nil {
		return err
	}
	l, err := net.Listen(network, address)
	if err != nil {
		return err
	}
	log.Printf("start listen, network: %s, address: %s", network, address)
	var tempDelay time.Duration // how long to sleep on accept failure
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Print("failed accept conn, retrying delay:", tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			log.Fatal("failed accept conn:", err)
		}
		go server.ServeCodec(jsonrpc.NewServerCodec(conn))
	}
}

func (s *KVServer) Get(req service.GetRequest,
	resp *service.GetResponse) error {
	node, err := s.db.Get(req.Key, func(g *kvdb.Getter) {
		if req.Getter != nil {
			g.Children = req.Getter.Children
			g.Start = req.Getter.Start
			g.Limit = req.Getter.Limit
		}
	})
	resp.Node = node
	return err
}

func (s *KVServer) GetMulti(req service.GetMultiRequest,
	resp *service.GetMultiResponse) error {
	nodeMap, err := s.db.GetMulti(req.Keys, func(g *kvdb.Getter) {
		if req.Getter != nil {
			g.Children = req.Getter.Children
			g.Start = req.Getter.Start
			g.Limit = req.Getter.Limit
		}
	})
	resp.NodeMap = nodeMap
	return err
}

func (s *KVServer) Set(req service.SetRequest,
	resp *service.SetResponse) error {
	return s.db.Set(req.Key, req.Value, func(s *kvdb.Setter) {
		if req.Setter != nil {
			s.ExpireAt = req.Setter.ExpireAt
		}
	})
}

func (s *KVServer) SetMulti(req service.SetMultiRequest,
	resp *service.SetMultiResponse) error {
	return s.db.SetMulti(req.KvPairs, func(s *kvdb.Setter) {
		if req.Setter != nil {
			s.ExpireAt = req.Setter.ExpireAt
		}
	})
}

func (s *KVServer) Delete(req service.DeleteRequest,
	resp *service.DeleteResponse) error {
	return s.db.Delete(req.Key, func(d *kvdb.Deleter) {
		if req.Deleter != nil {
			d.Children = req.Deleter.Children
		}
	})
}

func (s *KVServer) DeleteMulti(req service.DeleteMultiRequest,
	resp *service.DeleteMultiResponse) error {
	return s.db.DeleteMulti(req.Keys, func(d *kvdb.Deleter) {
		if req.Deleter != nil {
			d.Children = req.Deleter.Children
		}
	})
}

func (s *KVServer) Exist(req service.ExistRequest,
	resp *service.ExistResponse) error {
	has, err := s.db.Exist(req.Key)
	resp.Has = has
	return err
}

func (s *KVServer) Cleanup(_ service.CleanupRequest,
	_ *service.CleanupResponse) error {
	return s.db.Cleanup()
}
