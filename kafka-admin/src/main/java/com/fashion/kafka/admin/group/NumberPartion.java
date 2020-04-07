package com.fashion.kafka.admin.group;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class NumberPartion implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        String key = String.valueOf(o);
        if (Integer.parseInt(key) % 2 == 0) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public void close() {

    }
}
