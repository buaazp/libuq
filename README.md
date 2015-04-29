## UQ Cluster library

[UQ](https://github.com/buaazp/uq) is a distributed persistent message queue. Libuq is the programing language bindings for uq cluster.


### Progress

  Name  | Language | Author
:------:|:--------:|:--------:
[UQ](https://github.com/buaazp/uq)   |   HTTP   | built-in
[gouq](https://github.com/buaazp/libuq/gouq)  |  Go  | official
[juq](https://github.com/buaazp/libuq/juq)  |  Java  | [sumory](https://github.com/sumory)
  phpuq |  PHP     | coming soon
  pyuq  |  Python  | coming soon
   ...  |  ...     | TODO

### gouq

Gouq is the golang library for uq cluster. To use gouq, import the github package in your program:

```
import (
	"github.com/buaazp/libuq/gouq"
)
```

Then new a client:

```
// for single uq instance
c, err = uq.NewClient(protocol, ip, port)
// for multi instances in a cluster
c, err = uq.NewClientEtcd(protocol, etcdServers, cluster)
```

And use the queue methods like:

```
err = c.Add(topic, line, recycle)
err = c.Push(topic, value)
id, data, err := c.Pop(key)
err = c.Del(id)
```

Gouq library will automatically choose the instance online to deal with the queue methods. You needn't to worry about the connections and etcd results.

For more information, take a look at Godoc.org:

[https://godoc.org/github.com/buaazp/libuq/gouq](https://godoc.org/github.com/buaazp/libuq/gouq)

You can also read the sample code in `examples/go-uq.go`. To run the sample:

```
go build examples/go-uq.go
./go-uq -h
Usage of ./go-uq:
  -cluster="uq": cluster name in etcd
  -etcd="": etcd service location
  -ip="127.0.0.1": uq server ip address
  -line="x": line name
  -port=8808: uq server port
  -protocol="redis": frontend interface(redis, mc, http)
  -topic="foo": topic name
```

### juq

Juq is the Java library of uq cluster, Juq use redis client - [aredis](http://aredis.sourceforge.net/), and modify it to support commands from `uq`

```
//package org.aredis.cache
public enum RedisCommand {

    //###### modified by sumory for uq ######
    ADD("kk"), DEL("k"),
    //DEL("k@k", false, false, IntegerShardedResultHandler.instance),
    //###### modified by sumory for uq ######
    ...
}
```

Goto the [tests](juq/src/test/java/com/sumory/juq/JuqTest.java) for usage detail.


### Need Contributions

Implement all language bindings for uq cluster is a huge work to me. I really need your contributions to build a full-language uq cluster library. If you are good at some languages, php, python or any other, please make a PR to give some help.