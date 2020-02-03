package com.splio;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavouriteColor {
    public static void main(String[] args) {
        Logger LOG = LoggerFactory.getLogger(FavouriteColor.class.getName());

        Config kafkaConfig = ConfigFactory
                .load("resources/reference.conf")
                .getConfig("kafka");

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG));
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getString(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfig.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Stream from kafka
        KStream<String, String> colorsInput = builder.stream(kafkaConfig.getString("input-topic"));


        List<String> colorsToKeep = Arrays.asList("red", "green", "blue");

        KStream<String, String> userColors = colorsInput
                .filter((key, color) -> !color.isEmpty() && color.contains(","))
                // 1. isolate each space seperated values
//                .mapValues(value -> value.split(","))
                .selectKey((oldKey, keyValuesPairArray) -> keyValuesPairArray.split(",")[0])
                .mapValues(nameColorPair -> nameColorPair.split(",")[1])
              //  .mapValues(keyValuePair -> keyValuePair[1])
                // here the key is name and value is the colour
                // filter out only red, green, blue color
                .filter((key, color) -> colorsToKeep.contains(color));

        userColors.to(
                kafkaConfig.getString("user-colors-topic"),
                Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> userColorsTable = builder.table(
                kafkaConfig.getString("user-colors-topic"),
                Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> favouriteColors = userColorsTable
                .groupBy((name, color) -> new KeyValue<>(color, color))
                .count();

        favouriteColors.toStream().to(
                kafkaConfig.getString("output-topic"),
                Produced.with(Serdes.String(), Serdes.Long())
        );

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.cleanUp();
        streams.start();

//        LOG.info(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
