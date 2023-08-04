package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerParameters {

    public static void main(String[] args) {

        // 0. ����
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // ��������С 32M
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // ���δ�С 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // linger.ms 1����
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        // ѹ��
        // �����õ�ѹ��������:gzip, snappy, lz4, zstd
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // 1. ����������
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 2. ��������
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "atguigu" + i));
        }

        // 3. �ر���Դ
        kafkaProducer.close();

    }
}
