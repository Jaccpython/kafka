package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CustomProducerSync {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // 0. ����
        Properties properties = new Properties();

        // ���Ӽ�Ⱥ
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop102:9092");

        // ָ����Ӧ��key��value�����л�
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 1. ����kafka�����߶���
        // ""hello
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 2. ��������
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "atguigu" + i)).get();
        }

        // 3. �ر���Դ
        kafkaProducer.close();
    }
}
