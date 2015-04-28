package com.sumory.juq.conn;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.boon.etcd.EtcdClient;
import org.boon.etcd.Node;
import org.boon.etcd.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sumory.juq.JuqException;
import com.sumory.juq.thirdparty.RedisConnection;
import com.sumory.juq.thirdparty.RedisConnectionPair;

public class ConnRedis implements Connection {
    private final static Logger logger = LoggerFactory.getLogger(ConnRedis.class);
    // private static final Log logger = LogFactory.getLog(ConnRedis.class);
    private String addr;
    private EtcdClient etcdClient;
    private String etcdKey;
    private String[] addrs;
    private Map<String, RedisConnection> conns;

    private int maxRetry;

    public ConnRedis(String ip, int port) throws JuqException {
        String addr = ip + ":" + port;
        this.addr = addr;
        this.maxRetry = 1;
        this.updateConnPool();
    }

    public ConnRedis() {
    }

    public void updateConnPool() throws JuqException {
        this.conns = new HashMap<String, RedisConnection>();
        if (this.etcdClient == null) {
            RedisConnection conn;
            try {
                conn = this.dial(this.addr);
            }
            catch (JuqException e) {
                throw e;
            }

            this.addrs = new String[] { this.addr };
            this.conns.put(this.addr, conn);
        }
        else {
            this.dials();
        }
    }

    public void dials() throws JuqException {
        Response r = this.etcdClient.get(this.etcdKey + "/servers");
        List<Node> nodes = r.node().getNodes();

        if (nodes == null || nodes.size() == 0) {
            throw new JuqException("no UQ server registered in etcd");
        }

        this.addrs = new String[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            String[] parts = node.key().split("/");
            logger.info("parts: {}", parts.toString());

            String addr = parts[parts.length - 1];
            logger.info("server-{}:{}", i, addr);
            RedisConnection conn = this.dial(addr);

            this.addrs[i] = addr;
            this.conns.put(addr, conn);
        }

        if (this.conns.isEmpty()) {
            logger.error("all redis conn error");
            throw new JuqException("all redis conn error");
        }

        logger.info("redis conn succ: {}", this.conns.size());
    }

    public RedisConnection dial(String addr) throws JuqException {
        for (int i = 0; i < this.maxRetry; i++) {
            try {
                return new RedisConnection(addr);
            }
            catch (Exception e) {
                continue;
            }
        }

        throw new JuqException("cannot connect redis");
    }

    public RedisConnectionPair choose() {
        int n = this.addrs.length;

        if (n == 0) {
            return null;
        }
        else if (n == 1) {
            return new RedisConnectionPair(this.addrs[0], this.conns.get(this.addrs[0]));
        }

        int chosen = new Random(System.currentTimeMillis()).nextInt(n);
        String addr = this.addrs[chosen];
        return new RedisConnectionPair(addr, this.conns.get(addr));
    }

    public void add(String topic) throws JuqException {
        if (StringUtils.isBlank(topic)) {
            throw new JuqException("topic is null");
        }

        boolean success = false;

        int retry = 0;
        while (retry < this.maxRetry) {
            retry++;

            RedisConnectionPair p = this.choose();
            if (p == null) {
                logger.error("choose error, null");
            }
            else {
                String addr = p.getAddr();
                RedisConnection conn = p.getConn();

                logger.info("addr = {}", addr);

                String addResult = "";
                try {
                    addResult = conn.add(topic);
                    System.out.println(addResult);
                    if (addResult.contains("OK") || addResult.contains("Existed")) {
                        success = true;
                        break;
                    }
                }
                catch (Exception e) {
                    logger.error("add error", e);
                }
            }
        }
        if (!success)
            throw new JuqException("all conn add failed after retry.");
    }

    public void add(String topic, String line, long recycle) throws JuqException {
        if (StringUtils.isBlank(topic)) {
            throw new JuqException("topic is null");
        }

        boolean success = false;

        int retry = 0;
        while (retry < this.maxRetry) {
            retry++;

            RedisConnectionPair p = this.choose();
            if (p == null) {
                logger.error("choose error, null");
            }
            else {
                String addr = p.getAddr();
                RedisConnection conn = p.getConn();

                logger.info("addr = {}", p.getAddr());
                if ("".equals(line)) {
                    String addResult = "";
                    try {
                        addResult = conn.add(topic);
                        if (addResult.contains("OK") || addResult.contains("Existed")) {
                            success = true;
                            break;
                        }
                    }
                    catch (Exception e) {
                        logger.error("add error", e);
                    }
                }
                else {
                    String addResult = "";
                    String fullLineName = topic + "/" + line;
                    try {
                        addResult = conn.add(fullLineName, recycle + "s");
                        if (addResult.contains("OK") || addResult.contains("Existed")) {
                            success = true;
                            break;
                        }
                    }
                    catch (Exception e) {
                        logger.error("add error", e);
                    }
                }
            }

        }

        if (!success)
            throw new JuqException("all conn add failed after retry.");
    }

    public void push(String key, byte[] value) throws JuqException {
        if (StringUtils.isBlank(key) || value == null) {
            throw new JuqException("params null");
        }

        boolean success = false;

        int retry = 0;
        while (retry < this.maxRetry) {
            retry++;
            RedisConnectionPair p = this.choose();
            if (p == null) {
                logger.error("choose error, null");
            }
            else {
                String addr = p.getAddr();
                RedisConnection conn = p.getConn();

                logger.info("addr = {}", addr);

                String addResult = "";
                try {
                    addResult = conn.push(key, new String(value, "UTF-8"));
                    if (addResult.contains("OK")) {
                        success = true;
                        break;
                    }
                }
                catch (Exception e) {
                    logger.error("add error", e);
                }
            }

        }

        if (!success)
            throw new JuqException("all conn push failed after retry.");

    }

    public Map<String, Object> pop(String key) throws JuqException {
        if (StringUtils.isBlank(key)) {
            throw new JuqException("params null");
        }

        int retry = 0;
        int count = this.conns.size();
        while (retry < this.maxRetry) {
            retry++;

            int nomsg = 0;
            Iterator<Entry<String, RedisConnection>> it = this.conns.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, RedisConnection> entry = it.next();
                RedisConnection conn = entry.getValue();
                if (conn == null) {
                    logger.error("conn is null");
                    continue;
                }
                else {
                    try {
                        Object popResult = conn.pop(key);
                        if (popResult instanceof String) {
                            String strResult = (String) popResult;
                            if (strResult.contains("No Message")) {
                                nomsg++;
                            }
                            else if (strResult.contains("Not Existed")) {
                                logger.error("pop error:{}", popResult);
                            }
                        }
                        else if (popResult instanceof Object[]) {
                            List<Object> arrayResult = Arrays.asList((Object[]) popResult);

                            logger.info("pop result:{}", arrayResult);
                            byte[] value = arrayResult.get(0).toString().getBytes();
                            String cid = entry.getKey() + "/" + arrayResult.get(1);

                            Map<String, Object> result = new HashMap<String, Object>();
                            result.put("cid", cid);
                            result.put("value", value);
                            return result;
                        }

                    }
                    catch (Exception e) {
                        logger.error("pop error", e);
                    }
                    continue;
                }
            }

            if (nomsg == count) {
                throw new JuqException("No Message");
            }

        }

        throw new JuqException("all conn pop failed after retry.");
    }

    public void del(String key) throws JuqException {
        if (StringUtils.isBlank(key)) {
            throw new JuqException("key is null");
        }

        String[] parts = key.split("/", 2);
        if (parts.length < 2) {
            throw new JuqException("key illegal");
        }

        String addr = parts[0];
        String cid = parts[1];
        logger.info("addr = {}, cid={}", addr, cid);

        boolean success = false;

        int retry = 0;
        while (retry < this.maxRetry) {
            retry++;
            RedisConnection conn = this.conns.get(addr);

            String delResult = "";
            try {
                delResult = conn.del(cid);
                if (delResult.contains("OK")) {
                    success = true;
                    break;
                }
            }
            catch (Exception e) {
                logger.error("del error", e);
            }
        }
        if (!success)
            throw new JuqException("all conn del failed after retry.");
    }

    public void close() {
        Iterator<Entry<String, RedisConnection>> it = this.conns.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, RedisConnection> entry = it.next();
            RedisConnection conn = entry.getValue();
            //TODO:close
        }
    }

}
