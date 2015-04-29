#### java client for `uq`

use redis client - [aredis](http://aredis.sourceforge.net/), and modify it to support commands from `uq`

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

check [single node usage](src/test/java/com/sumory/juq/JuqTest.java) and [cluster usage](src/test/java/com/sumory/juq/EtcdTest.java) for detail.


