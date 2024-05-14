package com.ashok.kafka.consumer;

import com.ashok.kafka.model.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerListener {

    @KafkaListener(topics = "spring-topic-1", groupId = "spring-group", topicPartitions = {
            @TopicPartition(topic = "spring-topic-1", partitions = {"1"})
    })
    public void consumeMessage1(String message) {
        log.info("Consumed from kafka 1 : {}", message);
    }

    @KafkaListener(topics = "spring-topic-1", groupId = "spring-group", topicPartitions = {
            @TopicPartition(topic = "spring-topic-1", partitions = {"2"})
    })
    public void consumeMessage2(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                @Header(KafkaHeaders.OFFSET) Integer offset) {
        log.info("Consumed from kafka 2 : '{}' with topic : {} and offset : {}", message, topic, offset);
    }

    @RetryableTopic(attempts = "4")
    @KafkaListener(topics = "spring-topic-customer", groupId = "spring-group-customer")
    public void consumeCustomer(Customer customer, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                @Header(KafkaHeaders.OFFSET) Integer offset) {
        if (customer.getName() == null) {
            throw new RuntimeException("Consumer name is null");
        }
        log.info("Consumed from kafka customer event : {} from topic : {} and offset : {}",
                customer.toString(), topic, offset);
    }

    @DltHandler
    public void DLTHandler(Customer customer, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                           @Header(KafkaHeaders.OFFSET) Integer offset) {
        log.info("DLT received from kafka for customer : {} from topic : {} and offset : {}",
                customer.toString(), topic, offset);
    }

}
