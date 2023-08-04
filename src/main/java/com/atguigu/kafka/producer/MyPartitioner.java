package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements Partitioner {
    /**
     *
     * @param s 主题
     * @param key k
     * @param keys 序列化后的k
     * @param value v
     * @param values 序列化后的v
     * @param cluster
     * @return
     */
    @Override
    public int partition(String s, Object key, byte[] keys, Object value, byte[] values, Cluster cluster) {

        // 获取数据
        String msgValues = value.toString();

        int partition;
        if (msgValues.contains("atguigu")) {
            partition = 0;
        } else {
            partition = 1;
        }

        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
