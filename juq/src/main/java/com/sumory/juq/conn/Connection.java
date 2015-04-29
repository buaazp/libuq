package com.sumory.juq.conn;

import java.util.Map;

import com.sumory.juq.JuqException;

public interface Connection {
    public void add(String topic) throws JuqException;

    public void add(String topic, String line, long recycle) throws JuqException;

    public void push(String key, byte[] value) throws JuqException;

    public Map<String, Object> pop(String key) throws JuqException;

    public void del(String key) throws JuqException;

    public void close();

}
