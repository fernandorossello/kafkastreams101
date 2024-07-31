package com.example.kafkastreams101.streams;

import org.apache.kafka.clients.admin.NewTopic;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class StreamsUtils {

    public static final short REPLICATION_FACTOR = 3;
    public static final int PARTITIONS = 6;

    public static NewTopic createTopic(final String topicName){
        return new NewTopic(topicName, PARTITIONS, REPLICATION_FACTOR);
    }

    public static Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            properties.load(fis);
            return properties;
        }
    }
}
