package com.sumory.juq;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.sumory.juq.conn.ConnRedis;
import com.sumory.juq.conn.Connection;

public class JuqTest {
    @Test
    public void testBasics() {
        try {
            String host = "localhost";
            int port = 8808;
            String topic = "topic_from_juq1";
            String line = "line";
            String msg = "msg001";
            int maxRetry = 1;

            Connection conn = new ConnRedis(host, port, maxRetry);
            conn.add(topic);
            conn.add(topic, line, 10);
            conn.push(topic, msg.getBytes());
            Map<String, Object> result = conn.pop(topic + "/" + line);
            Assert.assertNotNull(result);
            Assert.assertNotNull(result.get("cid"));
            System.out.println((String) result.get("cid"));
            Assert.assertEquals(
                    true,
                    ((String) result.get("cid")).startsWith(host + ":" + port + "/" + topic + "/"
                            + line));
            Assert.assertArrayEquals(msg.getBytes(), (byte[]) result.get("value"));
        }
        catch (JuqException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testDel() {
        String host = "localhost";
        int port = 8808;
        String topic = "topic_from_juq2";
        String line = "line_with_recycle";
        String msg = "msg001";
        int maxRetry = 1;

        try {
            Connection conn = new ConnRedis(host, port, maxRetry);
            conn.add(topic);
            conn.add(topic, line, 10);
            conn.push(topic, msg.getBytes());
            Map<String, Object> result = conn.pop(topic + "/" + line);
            Assert.assertNotNull(result);
            Assert.assertNotNull(result.get("cid"));
            Assert.assertNotNull(result.get("value"));

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
