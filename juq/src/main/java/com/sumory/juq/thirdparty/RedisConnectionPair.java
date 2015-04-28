package com.sumory.juq.thirdparty;

public class RedisConnectionPair {
    private String addr;
    private RedisConnection conn;

    public RedisConnectionPair(String addr, RedisConnection conn) {
        this.addr = addr;
        this.conn = conn;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public RedisConnection getConn() {
        return conn;
    }

    public void setConn(RedisConnection conn) {
        this.conn = conn;
    }

}