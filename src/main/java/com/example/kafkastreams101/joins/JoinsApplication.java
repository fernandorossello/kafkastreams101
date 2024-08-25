package com.example.kafkastreams101.joins;

import com.example.kafkastreams101.helper.StreamsUtils;
import io.confluent.developer.avro.ApplianceOrder;
import io.confluent.developer.avro.CombinedOrder;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.developer.avro.User;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

@SpringBootApplication
public class JoinsApplication {

    public static void main(String[] args) {
        try {
            Properties properties = StreamsUtils.loadProperties();

            String appliancesTopic = properties.getProperty("stream_one.input.topic");
            String electronicsTopic = properties.getProperty("stream_two.input.topic");
            String usersTopic = properties.getProperty("table.input.topic");
            String outputTopic = properties.getProperty("joins.output.topic");


            Map<String, Object> configMap = StreamsUtils.propertiesToMap(properties);
            SpecificAvroSerde<ApplianceOrder> applianceSerde = getSpecificAvroSerde(configMap);
            SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);
            SpecificAvroSerde<CombinedOrder> combinedSerde = getSpecificAvroSerde(configMap);
            SpecificAvroSerde<User> userSerde = getSpecificAvroSerde(configMap);

            ValueJoiner<ApplianceOrder, ElectronicOrder, CombinedOrder> orderJoiner =
                    (applianceOrder, electronicOrder) -> CombinedOrder.newBuilder()
                            .setApplianceOrderId(applianceOrder.getOrderId())
                            .setApplianceId(applianceOrder.getApplianceId())
                            .setElectronicOrderId(electronicOrder.getOrderId())
                            .setTime(Instant.now().toEpochMilli())
                            .build();

            ValueJoiner<CombinedOrder, User, CombinedOrder> enrichmentJoiner = (combined, user) -> {
                if (user != null) {
                    combined.setUserName(user.getName());
                }
                return combined;
            };

            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, ApplianceOrder> applianceStream =
                    builder.stream(appliancesTopic, Consumed.with(Serdes.String(), applianceSerde))
                            .peek((key, value) -> System.out.println("Appliance stream incoming record key " + key + " value " + value));

            KStream<String, ElectronicOrder> electronicStream =
                    builder.stream(electronicsTopic, Consumed.with(Serdes.String(), electronicSerde))
                            .peek((key, value) -> System.out.println("Electronic stream incoming record " + key + " value " + value));

            KTable<String, User> userTable = builder.table(usersTopic, Materialized.with(Serdes.String(), userSerde));

            KStream<String, CombinedOrder> combinedStream =
                    applianceStream.join(
                            electronicStream,
                            orderJoiner,
                            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(30)),
                            StreamJoined.with(Serdes.String(),applianceSerde,electronicSerde)
                            )
                            .peek((key, value) -> System.out.println("Stream-Stream Join record key " + key + " value " + value));

            combinedStream.join(
                    userTable,
                    enrichmentJoiner,
                    Joined.with(Serdes.String(), combinedSerde,userSerde)
            ).peek((key, value) -> System.out.println("Stream-Table Join record key " + key + " value " + value))
                    .to(outputTopic, Produced.with(Serdes.String(),combinedSerde));

            KafkaStreams streams = new KafkaStreams(builder.build(), properties);
            TopicLoader.runProducer();

            streams.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(Map<String, Object> serdeConfig) {
        SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }
}
