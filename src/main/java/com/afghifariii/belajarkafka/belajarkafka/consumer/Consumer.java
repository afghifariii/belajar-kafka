package com.afghifariii.belajarkafka.belajarkafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

@Slf4j
public class Consumer {
    private String topicName;

    private String consumerGroup;

    private Properties properties;

    private KafkaConsumer<String, String> consumer;

    public Consumer(String topicName, String consumerGroup) {
        this.topicName = topicName;
        this.consumerGroup = consumerGroup;

        configProperties();
        configConsumer();

    }

    private void configProperties() {
        properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    }

    private void configConsumer() {
        consumer = new KafkaConsumer<>(properties);
    }

    public void run() {
        consumer.subscribe(Collections.singleton(topicName));

        Long timeout = 1000L;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);
            for (ConsumerRecord<String, String> record : records) {
                log.info("receive {}:{} from partition {}", record.key(), record.value(), record.partition());
            }
        }
    }

    public static void main(String[] args) {
        new Consumer("belajar-kafka", "belajar-kafka").run();
    }

}
