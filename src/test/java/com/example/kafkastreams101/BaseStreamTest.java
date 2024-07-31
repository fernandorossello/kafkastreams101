package com.example.kafkastreams101;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@SpringBootTest
public class BaseStreamTest {

    @Value("${kafka.bootstrap.servers}")
    protected String bootstrapServersProperty;

    @Autowired
    protected Admin admin;

    private NewTopic newTopic(String topic) {
        return new NewTopic(topic, 1, (short) 1);
    }

    protected void createTopics(String... topics) {
        List<NewTopic> newTopics =Arrays.stream(topics)
                .map(this::newTopic)
                .collect(Collectors.toList());

        admin.createTopics(newTopics);
    }
}
