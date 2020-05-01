package com.dmha.lab.kafkaretry.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Configuration
public class KafkaConfiguration {

    private static Logger LOGGER = LoggerFactory.getLogger(KafkaConfiguration.class);

    @Bean
    public ConsumerFactory<String, String> consumerFactory(){
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> KafkaListenerContainerFactory(){

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> KafkaRetryListenerContainerFactory(KafkaTemplate<String, String> kafkaTemplate, RetryTemplate retryTemplate){

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setRetryTemplate(retryTemplate());

        factory.setRecoveryCallback(context -> {
            ConsumerRecord record = (ConsumerRecord) context.getAttribute("record");
            LOGGER.error("Exceeded attempts, Sent message to error topic --message [{}]", record.value().toString());
            kafkaTemplate.send(record.topic().replace("retry", "error"), record.value().toString());

            return Optional.empty();
        });

        return factory;
    }

    @Bean
    public RetryTemplate retryTemplate(){

        RetryTemplate retryTemplate = new RetryTemplate();

        // You can use FixedBackOffPolicy too
        ExponentialBackOffPolicy exponentialBackOff = new ExponentialBackOffPolicy();
        exponentialBackOff.setInitialInterval(3000);
        exponentialBackOff.setMultiplier(2.0);
        exponentialBackOff.setMaxInterval(30000);
        retryTemplate.setBackOffPolicy(exponentialBackOff);

        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        simpleRetryPolicy.setMaxAttempts(10);
        retryTemplate.setRetryPolicy(simpleRetryPolicy);

        return retryTemplate;
    }

}
