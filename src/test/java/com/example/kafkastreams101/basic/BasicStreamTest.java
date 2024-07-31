package com.example.kafkastreams101.basic;

import com.example.kafkastreams101.BaseStreamTest;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.*;
import static org.awaitility.Awaitility.await;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BasicStreamTest extends BaseStreamTest {
    @Value("${kafka.basic.filter.input.topic}")
    String inputTopic;

    @Value("${kafka.basic.filter.input.topic}")
    String outputTopic;

    private Producer<String, String> producer;

    @BeforeAll
    void beforeAll() {
        createTopics(inputTopic,outputTopic);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersProperty);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        producer = new KafkaProducer<>(properties);
    }

    private List<String> rawRecords = List.of("orderNumber-1001",
            "orderNumber-5000",
            "orderNumber-999",
            "orderNumber-3330",
            "orderNumber-fer",
            "bogus-1",
            "bogus-2",
            "orderNumber-8400");

    private void produceMessagesTo(String topic) {
        var producerRecords = rawRecords.stream().map(r -> new ProducerRecord<>(topic, "akey-82q3kj23823jd", r)).collect(Collectors.toList());
        producerRecords.forEach( pr -> producer.send(pr));
    }

    @Test
    void test() {
        produceMessagesTo(inputTopic);

        await().pollInterval(2, SECONDS).atMost(10, SECONDS)
                .untilAsserted(() -> Assertions.assertTrue(false));
    }

}
