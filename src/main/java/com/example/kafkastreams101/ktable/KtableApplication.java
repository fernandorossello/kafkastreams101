package com.example.kafkastreams101.ktable;

import com.example.kafkastreams101.helper.AdminConfig;
import com.example.kafkastreams101.helper.StreamsUtils;
import com.example.kafkastreams101.helper.TopicLoader;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;
import java.util.Properties;

@SpringBootApplication
public class KtableApplication {

    private static final String FILTER_PREFIX = "orderNumber-";

    public static void main(String[] args) {

        try {
            Properties streamsProps = StreamsUtils.loadProperties();
            streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-ktable");
            final String inputTopic = streamsProps.getProperty("kafka.input.topic");
            final String outputTopic = streamsProps.getProperty("kafka.output.topic");
            AdminConfig.createTopics(List.of(inputTopic,outputTopic));


            StreamsBuilder builder = new StreamsBuilder();

            KTable<String, String> ktable = builder.table(inputTopic,
                    Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ktable-store")
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.String()));

            ktable
                .filter((key, value) -> value.contains(FILTER_PREFIX))
                .mapValues(value -> value.substring(value.indexOf("-") + 1))
                .filter((key, value) -> Long.parseLong(value) > 1000)
                .toStream()
                .peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

            KafkaStreams streams = new KafkaStreams(builder.build(),streamsProps);
            streams.start();

            TopicLoader.runProducer(inputTopic);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
