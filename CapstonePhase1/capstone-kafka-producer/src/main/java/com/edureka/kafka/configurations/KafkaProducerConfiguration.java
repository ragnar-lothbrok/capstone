package com.edureka.kafka.configurations;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.edureka.kafka.config.props.KafkaProperties;

@Configuration
public class KafkaProducerConfiguration {

	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerConfiguration.class);

	@Autowired
	private KafkaProperties kafkaProperties;

	@Bean
	public Serializer<String> stringKeySerializer() {
		return new StringSerializer();
	}

	@Bean
	public Producer<String, String> eventProducer() {
		logger.debug("Configuring Producer.");
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrap());
		props.put(ProducerConfig.ACKS_CONFIG, kafkaProperties.getAcks());
		props.put(ProducerConfig.RETRIES_CONFIG, kafkaProperties.getRetries());
		props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		logger.info("Event Producer created.");
		return producer;
	}
}
