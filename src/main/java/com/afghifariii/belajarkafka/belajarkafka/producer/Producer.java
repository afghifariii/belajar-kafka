package com.afghifariii.belajarkafka.belajarkafka.producer;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import rx.Observable;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


@Slf4j
public class Producer {
    private String topic;
    private Properties properties;
    private KafkaProducer<String, String> producer;

    public Producer(String topic) {
        this.topic = topic;

        configProperties();
        configProducer();

    }

    private void configProperties() {
        //Mandatory Producer Properties
        properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "0");
    }

    private void configProducer() {
        producer = new KafkaProducer<>(properties);
    }

    public Observable<RecordMetadata> send(String value) {
        String key = value;
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        return Observable.from(producer.send(record));

    }

    public void run() {
        Observable.interval(5, TimeUnit.SECONDS)
                .map(Object::toString)
                .flatMap(this::send)
                .subscribe(result -> {
                  log.info("Message sent to partition {}", result.partition());
        });

    }

    public static void main (String [] args) throws IOException {
        new Producer("belajar-kafka").run();

        System.in.read();
    }

}
