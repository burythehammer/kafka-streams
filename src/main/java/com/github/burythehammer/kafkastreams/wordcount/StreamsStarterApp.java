package com.github.burythehammer.kafkastreams.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-word-count");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream("word-count-input");

        final KTable<String, Long> wordCount = stream.mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(v -> Arrays.asList(v.split(" ")))
                .selectKey((k, v) -> v)
                .groupByKey()
                .count(Named.as("Counts"));

        wordCount.toStream(Named.as("word count output"));

        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        kafkaStreams.start();

        System.out.println(kafkaStreams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
