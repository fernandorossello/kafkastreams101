package com.example.kafkastreams101.helper;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class StreamsUtils {

    public static Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            properties.load(fis);
            return properties;
        }
    }
}
