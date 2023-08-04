package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerTranactions {

    public static void main(String[] args) {

        // 0. 配置
        Properties properties = new Properties();

        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop102:9092");

        // 指定对应的key和value的序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 事物ID
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional_id_01");

        // 1. 创建kafka生产者对象
        // ""hello
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 初始化/连接/提交/关闭 事物
        kafkaProducer.initTransactions();
        kafkaProducer.beginTransaction();
        try {

            // 2. 发送数据
            for (int i = 0; i < 500; i++) {
                kafkaProducer.send(new ProducerRecord<>("first", "atguigu" + i), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {

                        if (exception == null) {
                            System.out.println("主题：" + metadata.topic() + " 分区：" + metadata.partition());
                        }
                    }
                });
            }
        } catch (Exception e) {
            kafkaProducer.abortTransaction();
        } finally {
            kafkaProducer.commitTransaction();
        }

        // 3. 关闭资源
        kafkaProducer.close();
    }
}
