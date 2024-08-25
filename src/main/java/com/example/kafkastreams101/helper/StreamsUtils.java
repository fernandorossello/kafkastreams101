package com.example.kafkastreams101.helper;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamsUtils {

    public static Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            properties.load(fis);
            return properties;
        }
    }

    public static Map<String,Object> propertiesToMap(final Properties properties) {
        final Map<String, Object> configs = new HashMap<>();
        properties.forEach((key, value) -> configs.put((String)key, (String)value));
        return configs;
    }
}
