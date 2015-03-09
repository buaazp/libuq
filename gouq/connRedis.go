package uq

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/garyburd/redigo/redis"
)

type connRedis struct {
	addr       string
	etcdClient *etcd.Client
	etcdKey    string
	addrs      []string
	conns      map[string]redis.Conn
}

func newConnRedis(ip string, port int) (*connRedis, error) {
	addr := fmt.Sprintf("%s:%d", ip, port)
	c := new(connRedis)
	c.addr = addr
	err := c.updateConnPool()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func newConnRedisEtcd(etcdClient *etcd.Client, etcdKey string) (*connRedis, error) {
	c := new(connRedis)
	c.etcdClient = etcdClient
	c.etcdKey = etcdKey
	err := c.updateConnPool()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *connRedis) updateConnPool() error {
	c.conns = make(map[string]redis.Conn)
	if c.etcdClient == nil {
		conn, err := dial(c.addr)
		if err != nil {
			return err
		}
		c.addrs = []string{c.addr}
		c.conns[c.addr] = conn
	} else {
		return c.dials()
	}
	return nil
}

func dial(addr string) (redis.Conn, error) {
	for i := 0; i < maxRetry; i++ {
		conn, err := redis.DialTimeout("tcp", addr, 0, 1*time.Second, 1*time.Second)
		if err != nil {
			log.Printf("redis dial error: %s", err)
			// time.Sleep(1 * time.Second)
			continue
		}
		return conn, err
	}
	return nil, errors.New(addr + " dail failed after retry")
}

func (c *connRedis) dials() error {
	resp, err := c.etcdClient.Get(c.etcdKey+"/servers", true, false)
	if err != nil {
		log.Printf("etcd get error: %s", err)
		return err
	}
	if len(resp.Node.Nodes) == 0 {
		log.Printf("no UQ server registered in etcd")
		return errors.New("no UQ server registered in etcd")
	}

	c.addrs = make([]string, 0)
	for i, node := range resp.Node.Nodes {
		parts := strings.Split(node.Key, "/")
		log.Printf("parts: %v", parts)

		addr := parts[len(parts)-1]
		log.Printf("server-%d : %s", i, addr)
		conn, err := dial(addr)
		if err != nil {
			log.Printf("redis conn error: %s", err)
			continue
		}
		c.addrs = append(c.addrs, addr)
		c.conns[addr] = conn
	}

	if len(c.conns) == 0 {
		log.Printf("all redis conn error")
		return errors.New("all redis conn error")
	}
	log.Printf("redis conn succ: %v", len(c.conns))

	return nil
}

func (c *connRedis) findTopic(topic string) ([]string, error) {
	if c.etcdClient == nil {
		return []string{c.addr}, nil
	}

	resp, err := c.etcdClient.Get(topic, true, false)
	if err != nil {
		log.Printf("etcd get error: %s", err)
		return nil, err
	}
	if len(resp.Node.Nodes) == 0 {
		errmsg := fmt.Sprintf("no UQ server has topic[%s]", topic)
		return nil, errors.New(errmsg)
	}

	topicSvrs := make([]string, len(resp.Node.Nodes))
	for i, node := range resp.Node.Nodes {
		parts := strings.Split(node.Key, "/")
		log.Printf("parts: %v", parts)

		addr := parts[len(parts)-1]
		log.Printf("server-%d : %s", i, addr)

		topicSvrs[i] = addr
	}

	return topicSvrs, nil
}

func (c *connRedis) add(topic, line string, recycle time.Duration) error {
	if topic == "" {
		return errors.New("topic is nil")
	}

	updated := false
	for {
		retry := 0
		for retry < maxRetry {
			addr, conn, err := c.choose()
			if err != nil {
				log.Printf("choose error: %v", err)
			} else {
				log.Printf("addr = %s", addr)
				if line == "" {
					_, err := conn.Do("QADD", topic)
					if err != nil {
						if strings.Contains(err.Error(), "Existed") {
							return nil
						}
						log.Printf("add error: %v", err)
					}
				} else {
					fullLineName := topic + "/" + line
					_, err := conn.Do("QADD", fullLineName, recycle.String())
					if err != nil {
						if strings.Contains(err.Error(), "Existed") {
							return nil
						}
						log.Printf("add error: %v", err)
					}
				}
			}
			retry++
		}

		if !updated {
			err := c.updateConnPool()
			if err != nil {
				log.Printf("update conn pool error: %s", err)
				return err
			}
			updated = true
		} else {
			break
		}
	}

	errmsg := fmt.Sprintf("all conn add failed after retry.")
	return errors.New(errmsg)
}

func (c *connRedis) choose() (string, redis.Conn, error) {
	n := len(c.addrs)
	if n == 0 {
		return "", nil, errors.New("no uq address avilable")
	} else if n == 1 {
		addr := c.addrs[0]
		return addr, c.conns[addr], nil
	}

	rand.Seed(time.Now().UTC().UnixNano())
	chosen := rand.Intn(n)
	addr := c.addrs[chosen]
	conn, ok := c.conns[addr]
	if ok {
		log.Printf("%s is been choosed", addr)
		return addr, conn, nil
	}

	return "", nil, errors.New("conn choose failed.")
}

func (c *connRedis) push(key string, value []byte) error {
	updated := false
	for {
		retry := 0
		for retry < maxRetry {
			addr, conn, err := c.choose()
			if err != nil {
				log.Printf("choose error: %v", err)
			} else {
				log.Printf("addr = %s", addr)
				_, err := conn.Do("QPUSH", key, value)
				if err != nil {
					log.Printf("push error: %v", err)
				} else {
					return nil
				}
			}
			retry++
		}

		if !updated {
			err := c.updateConnPool()
			if err != nil {
				log.Printf("update conn pool error: %s", err)
				return err
			}
			updated = true
		} else {
			break
		}
	}

	errmsg := fmt.Sprintf("all conn push failed after retry.")
	return errors.New(errmsg)
}

func (c *connRedis) pop(key string) (string, []byte, error) {
	updated := false
	for {
		retry := 0
		count := len(c.conns)
		for retry < maxRetry {
			nomsg := 0
			for addr, conn := range c.conns {
				log.Printf("addr = %s", addr)
				reply, err := redis.Values(conn.Do("QPOP", key))
				if err != nil {
					if strings.Contains(err.Error(), "No Message") {
						nomsg++
					} else {
						log.Printf("pop error: %v", err)
					}
					continue
				}
				id := uint64(reply[0].(int64))
				value := []byte(reply[1].([]byte))
				cid := fmt.Sprintf("%s/%s/%d", addr, key, id)
				return cid, value, nil
			}
			if nomsg == count {
				return "", nil, errors.New("No Message")
			}
			retry++
		}

		if !updated {
			err := c.updateConnPool()
			if err != nil {
				log.Printf("update conn pool error: %s", err)
				return "", nil, err
			}
			updated = true
		} else {
			break
		}
	}

	errmsg := fmt.Sprintf("all conn pop failed after retry.")
	return "", nil, errors.New(errmsg)
}

func (c *connRedis) del(key string) error {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return errors.New("key illegal")
	}
	addr := parts[0]
	cid := parts[1]

	retry := 0
	for retry < maxRetry {
		conn, ok := c.conns[addr]
		if !ok {
			return errors.New(addr + " is not avilable")
		}
		_, err := conn.Do("QDEL", cid)
		if err != nil {
			log.Printf("del error: %v", err)
		} else {
			return nil
		}
		retry++
	}

	errmsg := fmt.Sprintf("all conn del failed after %d retry.", retry)
	return errors.New(errmsg)
}

func (c *connRedis) close() {
	for _, conn := range c.conns {
		conn.Close()
	}
}
