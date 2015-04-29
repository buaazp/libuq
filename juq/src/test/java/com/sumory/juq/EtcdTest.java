package com.sumory.juq;

import static org.boon.Boon.puts;

import java.net.URI;
import java.util.Map;

import org.boon.etcd.ClientBuilder;
import org.boon.etcd.Etcd;
import org.boon.etcd.Response;
import org.junit.Assert;
import org.junit.Test;

import com.sumory.juq.conn.ConnRedis;

public class EtcdTest {
    @Test
    public void testEtcd() {
        Etcd client = ClientBuilder.builder().hosts(//
                URI.create("http://localhost:4001"),//
                URI.create("http://localhost:4002"),//
                URI.create("http://localhost:4003")).createClient();

        Response r;
        r = client.delete("foo");
        puts(r);
        r = client.get("foo");
        puts(r);
        Assert.assertFalse(r.successful());

        r = client.set("foo", "bar");
        Assert.assertTrue(r.successful());

        r = client.get("foo");
        Assert.assertEquals(r.node().getValue(), "bar");
    }

    @Test
    public void testNewEtcdConn() {
        Etcd etcdClient = ClientBuilder.builder().hosts(//
                URI.create("http://localhost:4001"),//
                URI.create("http://localhost:4002"),//
                URI.create("http://localhost:4003")).createClient();

        try {
            int maxRetry = 1;
            Response r;
            r = etcdClient.get("uq");
            ConnRedis conn = new ConnRedis(etcdClient, "uq", maxRetry);
        }
        catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testBasics() {
        Etcd etcdClient = ClientBuilder.builder().hosts(//
                URI.create("http://localhost:4001"),//
                URI.create("http://localhost:4002"),//
                URI.create("http://localhost:4003")).createClient();

        try {
            int maxRetry = 1;
            Response r = etcdClient.get("uq");
            ConnRedis conn = new ConnRedis(etcdClient, "uq", maxRetry);

            String topic = "topic_from_juq1";
            String line = "line";
            String msg = "msg001";

            conn.add(topic);
            conn.add(topic, line, 10);
            conn.push(topic, msg.getBytes());
            Map<String, Object> result = conn.pop(topic + "/" + line);
            Assert.assertNotNull(result);
            Assert.assertNotNull(result.get("cid"));
            System.out.println((String) result.get("cid"));
            Assert.assertArrayEquals(msg.getBytes(), (byte[]) result.get("value"));

            String fullId = (String) result.get("cid");
            System.out.println("fullId:" + fullId);
            conn.del(fullId);

        }
        catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
