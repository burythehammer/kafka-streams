package com.github.burythehammer.kafka.streams.favouritecolour;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FavouriteColourApp {

    private static final Logger logger = LoggerFactory.getLogger("KafkaStreamFavouriteColourLogger");

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> faveColours = builder.stream("favourite-colour-input");

        faveColours.filter((k, v) -> (v.split(",", -1).length) - 1 == 1)
                .selectKey((k, v) -> v.split(",")[0])
                .mapValues(String::toLowerCase)
                .mapValues(String::trim)
                .mapValues(v -> v.split(",")[1])
                .peek((k, v) -> logger.info("Person: \"" + k + "\", Colour: \"" + v + "\""))
                .to(Serdes.String(), Serdes.String(), "favourite-colour-by-person");


        KTable<String, String> colourNumbers = builder.table("favourite-colour-by-person");
        KTable<String, Long> favouriteColours = colourNumbers
                .groupBy((k, v) -> new KeyValue<>(v, v))
                .count("CountsByColours");

        favouriteColours.to(Serdes.String(), Serdes.Long(),"favourite-colour-output");


        KafkaStreams streams = new KafkaStreams(builder, config);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
