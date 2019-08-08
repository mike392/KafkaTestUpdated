package com.example.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Properties;

import static sun.nio.cs.Surrogate.MIN;

@SpringBootApplication
public class KafkaApplication {
//	bin\windows\kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mytopic
//	kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning --topic myouttopic
//	kafka-console-producer --topic mytopic --broker-list localhost:9092 --property "parse.key=true" --property "key.separator=:"
	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
		String inputTopic = "mytopic";
		String inputTopic1 = "mytopic1";
		String outputTopic = "myouttopic";
		String bootstrapServers = "localhost:9092";
		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(
				StreamsConfig.APPLICATION_ID_CONFIG,
				"wordcount-live-test");

		streamsConfiguration.put(
				StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				bootstrapServers);
		streamsConfiguration.put(
				StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
				Serdes.String().getClass().getName());
		streamsConfiguration.put(
				StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
				Serdes.String().getClass().getName());
//		streamsConfiguration.put(
//				StreamsConfig.STATE_DIR_CONFIG,
//				TestUtils.tempDirectory().getAbsolutePath());
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
		KStream<String, String> input1 = builder.stream(inputTopic1, Consumed.with(Serdes.String(), Serdes.String()));
		// KTable<String, String> textLines = builder.table(inputTopic);
		// KTable<String, String> textLines1 = builder.table(inputTopic1);
//		Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
//
//		textLines
//				.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+"))).to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
//		textLines.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

//		KTable<String, String> result = textLines.join(textLines1, (value1, value2) -> value2);
		System.out.println("dfgdfgdf");
		input.join(input1, (v1, v2) -> {
			System.out.println("dgfdgf");
			return v1 + "+" + v2;
		}, JoinWindows.of(1000000)).to(outputTopic);
//		input1.to(outputTopic);
//		result.toStream().foreach((key, value) -> System.out.println(key + "/" + value));
		KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

		streams.start();
	}

}
