package com.example.kafkastreams101;

import com.example.kafkastreams101.streams.TopicLoader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class Kafkastreams101Application {

    public static void main(String[] args) {
        SpringApplication.run(Kafkastreams101Application.class, args);
        try {
            TopicLoader.runProducer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
