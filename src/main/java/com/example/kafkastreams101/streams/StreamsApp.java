package com.example.kafkastreams101.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

@Configuration
public class StreamsApp {

    public static final String FILTER_PREFIX = "orderNumber-";

    @Bean
    public KStream<String, String> filterStream() throws IOException {
        Properties streamsProps = getClusterProperties();

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("test.input.topic");
        final String outputTopic = streamsProps.getProperty("test.output.topic");

        KStream<String,String> stream = builder.stream(inputTopic, Consumed.with(Serdes.String(),Serdes.String()));

        stream
            .peek(((key, value) -> System.out.println("Input: " + key + " - "+ value)))
            .filter((key, value) -> value != null && value.contains(FILTER_PREFIX) )
            .peek(((key, value) -> System.out.println("Output: " + key + " - "+ value)))
            .to(outputTopic, Produced.with(Serdes.String(),Serdes.String()));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.start();

        return stream;
    }


    private Properties getClusterProperties() throws IOException {
        Properties streamsProps = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            streamsProps.load(fis);
        }

        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");

        return streamsProps;
    }
}
