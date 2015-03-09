package uq

import (
	"errors"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

const (
	ProtoHttp  string = "http"
	ProtoMc    string = "mc"
	ProtoRedis string = "redis"
	maxRetry   int    = 3
)

type Client struct {
	uqConn Conn
}

func NewClient(uqProtocol string, uqIp string, uqPort int) (*Client, error) {
	client := new(Client)
	var uqConn Conn
	var err error
	if uqProtocol == ProtoRedis {
		uqConn, err = newConnRedis(uqIp, uqPort)
		if err != nil {
			return nil, err
		}
	} else if uqProtocol == ProtoMc {
		uqConn, err = newConnMc(uqIp, uqPort)
		if err != nil {
			return nil, err
		}
	} else if uqProtocol == ProtoHttp {
		uqConn, err = newConnHttp(uqIp, uqPort)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("protocol unsupported")
	}
	client.uqConn = uqConn

	return client, nil
}

func NewClientEtcd(uqProtocol string, etcdAddr []string, etcdKey string) (*Client, error) {
	client := new(Client)
	if len(etcdAddr) > 0 {
		etcdClient := etcd.NewClient(etcdAddr)
		var uqConn Conn
		var err error
		if uqProtocol == ProtoRedis {
			uqConn, err = newConnRedisEtcd(etcdClient, etcdKey)
			if err != nil {
				return nil, err
			}
		} else if uqProtocol == ProtoMc {
			uqConn, err = newConnMcEtcd(etcdClient, etcdKey)
			if err != nil {
				return nil, err
			}
		} else if uqProtocol == ProtoHttp {
			uqConn, err = newConnHttpEtcd(etcdClient, etcdKey)
			if err != nil {
				return nil, err
			}
		}
		client.uqConn = uqConn
	} else {
		return nil, errors.New("etcd address nil")
	}
	return client, nil
}

func (c *Client) Add(topic, line string, recycle time.Duration) error {
	return c.uqConn.add(topic, line, recycle)
}

func (c *Client) Push(key string, value []byte) error {
	return c.uqConn.push(key, value)
}

func (c *Client) Pop(key string) (string, []byte, error) {
	return c.uqConn.pop(key)
}

func (c *Client) Del(key string) error {
	return c.uqConn.del(key)
}

func (c *Client) Close() {
	c.uqConn.close()
}
