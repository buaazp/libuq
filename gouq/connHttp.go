package uq

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

type connHttp struct {
	addr       string
	etcdClient *etcd.Client
	etcdKey    string
	addrs      []string
	http.Client
}

type createRequest struct {
	TopicName string
	LineName  string
	Recycle   time.Duration
}

func newConnHttp(ip string, port int) (*connHttp, error) {
	addr := fmt.Sprintf("%s:%d", ip, port)
	c := new(connHttp)
	c.addr = addr
	err := c.updateConnPool()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func newConnHttpEtcd(etcdClient *etcd.Client, etcdKey string) (*connHttp, error) {
	c := new(connHttp)
	c.etcdClient = etcdClient
	c.etcdKey = etcdKey
	err := c.updateConnPool()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *connHttp) updateConnPool() error {
	if c.etcdClient == nil {
		c.addrs = []string{c.addr}
	} else {
		return c.dials()
	}
	return nil
}

func (c *connHttp) dials() error {
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
		c.addrs = append(c.addrs, addr)
	}

	log.Printf("http conn succ: %v", len(c.addrs))

	return nil
}

// func (c *connHttp) findTopic(topic string) ([]string, error) {
// 	if c.etcdClient == nil {
// 		return []string{c.addr}, nil
// 	}

// 	resp, err := c.etcdClient.Get(topic, true, false)
// 	if err != nil {
// 		log.Printf("etcd get error: %s", err)
// 		return nil, err
// 	}
// 	if len(resp.Node.Nodes) == 0 {
// 		errmsg := fmt.Sprintf("no UQ server has topic[%s]", topic)
// 		return nil, errors.New(errmsg)
// 	}

// 	topicSvrs := make([]string, len(resp.Node.Nodes))
// 	for i, node := range resp.Node.Nodes {
// 		parts := strings.Split(node.Key, "/")
// 		log.Printf("parts: %v", parts)

// 		addr := parts[len(parts)-1]
// 		log.Printf("server-%d : %s", i, addr)

// 		topicSvrs[i] = addr
// 	}

// 	return topicSvrs, nil
// }

func (c *connHttp) add(topic, line string, recycle time.Duration) error {
	if topic == "" {
		return errors.New("topic is nil")
	}

	updated := false
	for {
		retry := 0
		for retry < maxRetry {
			addr, err := c.choose()
			if err != nil {
				log.Printf("choose error: %v", err)
			} else {
				log.Printf("addr = %s", addr)
				recycleStr := recycle.String()
				reqFormBody := "topic=" + topic + "&line=" + line + "&recycle=" + recycleStr
				// bf := bytes.NewBufferString(reqFormBody)
				// b.req.Body = ioutil.NopCloser(bf)
				// b.req.ContentLength = int64(len(t))
				b := bytes.NewBufferString(reqFormBody)
				req, err := http.NewRequest("PUT", "http://"+addr+"/v1/queues", b)
				if err != nil {
					log.Printf("new req error: %v", err)
				} else {
					req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
					resp, err := c.Do(req)
					if err != nil {
						log.Printf("do error: %v", err)
					} else {
						if resp.StatusCode == http.StatusCreated {
							return nil
						} else {
							respBody, err := ioutil.ReadAll(resp.Body)
							if err != nil {
								log.Printf("readall error: %v", err)
							} else {
								respStr := string(respBody)
								if strings.Contains(respStr, "Existed") {
									return nil
								}
								log.Printf("add error: %s", respStr)
							}
						}
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

func (c *connHttp) choose() (string, error) {
	n := len(c.addrs)
	if n == 0 {
		return "", errors.New("no uq address avilable")
	} else if n == 1 {
		addr := c.addrs[0]
		return addr, nil
	}

	rand.Seed(time.Now().UTC().UnixNano())
	chosen := rand.Intn(n)
	addr := c.addrs[chosen]
	log.Printf("%s is been choosed", addr)
	return addr, nil
}

func (c *connHttp) push(key string, value []byte) error {
	updated := false
	for {
		retry := 0
		for retry < maxRetry {
			addr, err := c.choose()
			if err != nil {
				log.Printf("choose error: %v", err)
			} else {
				log.Printf("addr = %s", addr)
				reqFormBody := "value=" + string(value)
				b := bytes.NewBufferString(reqFormBody)
				req, err := http.NewRequest("POST", "http://"+addr+"/v1/queues/"+key, b)
				if err != nil {
					log.Printf("new req error: %v", err)
				} else {
					req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
					resp, err := c.Do(req)
					if err != nil {
						log.Printf("do error: %v", err)
					} else {
						if resp.StatusCode == http.StatusNoContent {
							return nil
						} else {
							body, err := ioutil.ReadAll(resp.Body)
							if err != nil {
								log.Printf("readall error: %v", err)
							} else {
								log.Printf("push error: %s", string(body))
							}
						}
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

	errmsg := fmt.Sprintf("all conn push failed after retry.")
	return errors.New(errmsg)
}

func (c *connHttp) pop(key string) (string, []byte, error) {
	updated := false
	for {
		retry := 0
		count := len(c.addrs)
		for retry < maxRetry {
			nomsg := 0
			for _, addr := range c.addrs {
				log.Printf("addr = %s", addr)
				req, err := http.NewRequest("GET", "http://"+addr+"/v1/queues/"+key, nil)
				if err != nil {
					log.Printf("new req error: %v", err)
				} else {
					resp, err := c.Do(req)
					if err != nil {
						log.Printf("do error: %v", err)
					} else {
						if resp.StatusCode == http.StatusNotFound {
							nomsg++
						} else {
							value, err := ioutil.ReadAll(resp.Body)
							if err != nil {
								log.Printf("readall error: %v", err)
							}

							if resp.StatusCode == http.StatusOK {
								id := resp.Header.Get("X-UQ-ID")
								cid := fmt.Sprintf("%s/%s", addr, id)
								return cid, value, nil
							} else {
								log.Printf("pop error: %s", string(value))
							}
						}
					}
				}
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

func (c *connHttp) del(key string) error {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) < 2 {
		return errors.New("key illegal")
	}
	addr := parts[0]
	cid := parts[1]

	retry := 0
	for retry < maxRetry {
		req, err := http.NewRequest("DELETE", "http://"+addr+"/v1/queues/"+cid, nil)
		if err != nil {
			log.Printf("new req error: %v", err)
		} else {
			resp, err := c.Do(req)
			if err != nil {
				log.Printf("do error: %v", err)
			} else {
				if resp.StatusCode == http.StatusNoContent {
					return nil
				} else {
					body, err := ioutil.ReadAll(resp.Body)
					if err != nil {
						log.Printf("readall error: %v", err)
					} else {
						log.Printf("del error: %s", string(body))
					}
				}
			}
		}
		retry++
	}

	errmsg := fmt.Sprintf("all conn del failed after %d retry.", retry)
	return errors.New(errmsg)
}

func (c *connHttp) close() {
	c.addrs = nil
}
