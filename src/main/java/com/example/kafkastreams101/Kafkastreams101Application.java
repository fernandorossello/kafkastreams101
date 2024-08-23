package com.example.kafkastreams101;

import com.example.kafkastreams101.streams.TopicLoader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

@SpringBootApplication
public class Kafkastreams101Application {

    public static final String FILTER_PREFIX = "orderNumber-";

    public static void main(String[] args) {
        try {
            Properties streamsProps = new Properties();
            try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
                streamsProps.load(fis);
            }

            streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");


            StreamsBuilder builder = new StreamsBuilder();
            final String inputTopic = streamsProps.getProperty("kafka.basic.filter.input.topic");
            final String outputTopic = streamsProps.getProperty("kafka.basic.filter.output.topic");

            KStream<String, String> stream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

            stream
                .peek(((key, value) -> System.out.println("Input: " + key + " - " + value)))
                .filter((key, value) -> value != null && value.contains(FILTER_PREFIX))
                .mapValues(value -> value.replaceAll(FILTER_PREFIX,"Fer test mapping -"))
                .map((key,value) -> KeyValue.pair("New key",value))
                .peek(((key, value) -> System.out.println("Output: " + key + " - " + value)))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));


            KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
            kafkaStreams.start();

            TopicLoader.runProducer(inputTopic);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
