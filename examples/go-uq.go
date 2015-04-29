package main

import (
	"flag"
	"log"
	"strings"
	"time"

	"github.com/buaazp/libuq/gouq"
)

var (
	ip       string
	port     int
	protocol string
	etcd     string
	cluster  string
	topic    string
	line     string
)

func init() {
	flag.StringVar(&ip, "ip", "127.0.0.1", "uq server ip address")
	flag.IntVar(&port, "port", 8808, "uq server port")
	flag.StringVar(&protocol, "protocol", "redis", "frontend interface(redis, mc, http)")
	flag.StringVar(&etcd, "etcd", "", "etcd service location")
	flag.StringVar(&cluster, "cluster", "uq", "cluster name in etcd")
	flag.StringVar(&topic, "topic", "foo", "topic name")
	flag.StringVar(&line, "line", "x", "line name")
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	flag.Parse()

	var c *uq.Client
	var err error
	if etcd == "" {
		c, err = uq.NewClient(protocol, ip, port)
	} else {
		etcdServers := strings.Split(etcd, ",")
		c, err = uq.NewClientEtcd(protocol, etcdServers, cluster)
	}
	if err != nil {
		log.Printf("new client error: %s", err)
		return
	}
	log.Printf("new client succ: %v", err)

	recycle := 10 * time.Second
	err = c.Add(topic, "", 0)
	if err != nil {
		if !strings.Contains(err.Error(), "Existed") {
			log.Printf("add error: %s", err)
			return
		}
	}
	log.Printf("add succ: %v", err)

	err = c.Add(topic, line, recycle)
	if err != nil {
		if !strings.Contains(err.Error(), "Existed") {
			log.Printf("add error: %s", err)
			return
		}
	}
	log.Printf("add succ: %v", err)

	value := []byte("1111111")
	err = c.Push(topic, value)
	if err != nil {
		log.Printf("push error: %s", err)
		return
	}
	log.Printf("push succ: %v", err)

	key := topic + "/" + line
	id, data, err := c.Pop(key)
	if err != nil {
		log.Printf("pop error: %s", err)
		return
	}
	log.Printf("pop succ: %s %s", id, string(data))

	err = c.Del(id)
	if err != nil {
		log.Printf("del error: %s", err)
		return
	}
	log.Printf("del succ: %v", err)

	log.Printf("all test finished.")
}
