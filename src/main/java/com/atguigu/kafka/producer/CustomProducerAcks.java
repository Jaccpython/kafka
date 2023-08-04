package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerAcks {

    public static void main(String[] args) {

        // 0. ����
        Properties properties = new Properties();

        // ���Ӽ�Ⱥ
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop102:9092");

        // ָ����Ӧ��key��value�����л�
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        // ���Դ���
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 1. ����kafka�����߶���
        // ""hello
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 2. ��������
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "atguigu" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                    if (exception == null) {
                        System.out.println("���⣺" + metadata.topic() + " ������" + metadata.partition());
                    }
                }
            });
        }

        // 3. �ر���Դ
        kafkaProducer.close();
    }
}
