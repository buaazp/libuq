package com.sumory.juq.thirdparty;

import java.util.concurrent.Future;

import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisConnection {
    private final static Logger logger = LoggerFactory.getLogger(RedisConnection.class);
    private AsyncRedisClient aRedisClient;

    public RedisConnection(String addr) {
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        aRedisClient = f.getClient(addr);
    }

    public String add(String topic) throws Exception {
        Future<RedisCommandInfo> future = this.aRedisClient.submitCommand(RedisCommand.ADD, topic);
        try {
            String val = (String) future.get().getResult();
            logger.info("Got `ADD` back, topic={} result={}", topic, val);
            return val;
        }
        catch (Exception e) {
            throw e;
        }
    }

    public String add(String fullLineName, String recycle) throws Exception {
        Future<RedisCommandInfo> future = this.aRedisClient.submitCommand(RedisCommand.ADD,
                fullLineName, recycle);
        try {
            String val = (String) future.get().getResult();
            logger.info("Got `ADD` back, fullLineName={} recycle={} result={}", fullLineName,
                    recycle, val);
            return val;
        }
        catch (Exception e) {
            throw e;
        }
    }

    public String push(String key, String value) throws Exception {
        Future<RedisCommandInfo> future = this.aRedisClient.submitCommand(RedisCommand.SET, key,
                value);
        try {
            String val = (String) future.get().getResult();
            logger.info("Got `SET` back, key={} result={}", key, val);
            return val;
        }
        catch (Exception e) {
            throw e;
        }
    }

    public Object pop(String key) throws Exception {
        Future<RedisCommandInfo> future = this.aRedisClient.submitCommand(RedisCommand.GET, key);

        try {
            Object result = future.get().getResult();
            logger.info("Got `GET` back, key={} result={}", key, result);
            return result;
        }
        catch (Exception e) {
            throw e;
        }
    }

    public String del(String key) throws Exception {
        Future<RedisCommandInfo> future = this.aRedisClient.submitCommand(RedisCommand.DEL, key);

        try {
            String result = (String) future.get().getResult();
            logger.info("Got `DEL` back, key={} result={}", key, result);
            return result;
        }
        catch (Exception e) {
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        String attr = "127.0.0.1:8808";
        RedisConnection conn = new RedisConnection(attr);
        try {
            conn.add("test_by_java_client1");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }
}
