package com.sumory.juq;

import static org.boon.Boon.puts;

import java.net.URI;

import org.boon.etcd.ClientBuilder;
import org.boon.etcd.Etcd;
import org.boon.etcd.Response;
import org.junit.Assert;
import org.junit.Test;

public class EtcdTest {
    @Test
    public void test() {
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
}
