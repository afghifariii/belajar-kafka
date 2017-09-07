package com.afghifariii.belajarkafka.belajarkafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

@EnableKafka
@SpringBootApplication
public class SpringConsumer {

    @Slf4j
    @Component
    public static class ConsumerApp {
        @KafkaListener(topics = "topic-belajar-kafka", group = "belajar-kafka")
        public void listen(ConsumerRecord<String, String> record){
            log.info("Recieve {}:{} from partition {}", record.key(), record.value(), record.partition());
        }
    }

    public static void main(String [] args) throws IOException{
        SpringApplication.run(SpringConsumer.class, args);

        System.in.read();
    }
}
