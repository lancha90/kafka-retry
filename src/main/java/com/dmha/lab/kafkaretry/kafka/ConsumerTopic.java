package com.dmha.lab.kafkaretry.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ConsumerTopic {

    private static Logger LOGGER = LoggerFactory.getLogger(ConsumerTopic.class);

    private final String TOPIC = "event-topic";

    private final String TOPIC_RETRY = "event-topic-retry";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(topics = TOPIC, groupId = "main-group", containerFactory = "KafkaListenerContainerFactory")
    private void consumer(String message){

        try{
            if(message.contains("retry")){
                throw new IllegalArgumentException("Invalid Message!");
            }
            LOGGER.info("Message [{}] is Ok", message);
        } catch (Exception e){
            LOGGER.error("Invalid Message, Sent To Retry --message: [{}]", message);
            kafkaTemplate.send( TOPIC_RETRY, message);
        }
    }


    @KafkaListener(topics = TOPIC_RETRY, groupId = "main-group", containerFactory = "KafkaRetryListenerContainerFactory")
    private void retry(String message){

        LOGGER.info("New attempt to process the retry");

        try{
            if(message.contains("error")){
                throw new IllegalArgumentException("Invalid Message!");
            }
        } catch (Exception e){
            LOGGER.error("Retry Failed --message: [{}]", message);
            throw e;
        }

    }

}
