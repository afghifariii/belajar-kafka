package com.afghifariii.belajarkafka.belajarkafka.producer;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import rx.Observable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@EnableKafka
@SpringBootApplication
public class SpringProducer {

    public static void main (String [] args) throws IOException{
        ConfigurableApplicationContext context = SpringApplication.run(SpringProducer.class, args);

        ProducerApp producerApp = context.getBean(ProducerApp.class);
        producerApp.run("spring_topic");

        System.in.read();
    }

    @Slf4j
    @Component
    public static class ProducerApp{

        @Autowired
        private KafkaTemplate<String, String> kafkaTemplate;

        public void run(String topicName) {
            Observable.interval(1, TimeUnit.SECONDS)
                    .map(Object::toString)
                    .flatMap(value -> send(topicName, value))
                    .subscribe(result -> {
                        log.info("Success send message to partition {}", result.getRecordMetadata().partition());
                    });
        }

        public Observable<SendResult<String, String>> send(String topicName, String value) {
            String key = value;
            return Observable.from(kafkaTemplate.send(topicName, key, value));
        }
    }
}