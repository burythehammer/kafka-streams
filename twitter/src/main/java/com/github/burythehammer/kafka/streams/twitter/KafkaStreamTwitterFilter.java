package com.github.burythehammer.kafka.streams.twitter;

import java.util.Properties;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaStreamTwitterFilter {
    private static final Logger logger = LoggerFactory.getLogger("KafkaStreamTwitterLogger");

    private static final String SOURCE_TOPIC = "twitter_tweets";
    private static final String DESTINATION_TOPIC = "popular_tweets";
    private static final String KAFKA_SERVER_URL = "localhost:9092";
    private static final String APPLICATION_ID = "kafka-streams";

    private static final int POPULAR_USER_THRESHOLD = 10000;

    public static void main(String[] args){

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputTopic = streamsBuilder.stream(SOURCE_TOPIC);

        final KStream<String, String> filteredStream = inputTopic.filter((k, v) -> {
            int userFollowers = extractUserFollowersInTweet(v);
            return userFollowers > POPULAR_USER_THRESHOLD;
        });

        filteredStream.to(DESTINATION_TOPIC);

        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        logger.info("hello");

        kafkaStreams.start();
    }

    private static int extractUserFollowersInTweet(String jsonTweet){
        try {
            return JsonParser.parseString(jsonTweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}

