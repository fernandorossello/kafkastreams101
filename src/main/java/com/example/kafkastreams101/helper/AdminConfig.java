package com.example.kafkastreams101.helper;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class AdminConfig {

    public static void createTopics(List<String> topics) throws IOException {
        Properties properties = getProperties();

        var admin = Admin.create(properties);

        admin.createTopics(topics.stream().map(name -> new NewTopic(name, 1,Short.parseShort("1"))).collect(Collectors.toList()));
    }

    private static Properties  getProperties() throws IOException {
        Properties properties = StreamsUtils.loadProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }
}
