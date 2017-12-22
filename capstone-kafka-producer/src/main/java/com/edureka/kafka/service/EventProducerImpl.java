package com.edureka.kafka.service;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.edureka.kafka.api.EventProducerApi;
import com.edureka.kafka.config.props.KafkaProperties;
import com.edureka.kafka.producer.EventCallBack;
import com.edureka.kafka.utility.FileUtility;

@Service
public class EventProducerImpl implements EventProducerApi {

	@Autowired
	private KafkaProperties kafkaProperties;

	private static final Logger LOGGER = LoggerFactory.getLogger(EventProducerImpl.class);

	@PostConstruct
	private void test() {
		FileUtility.readFile(kafkaProperties.getTransactionTopic().getFilePath(),
				kafkaProperties.getTransactionTopic().getAvroSchemaPath(),
				kafkaProperties.getTransactionTopic().getTopic(), this);

		FileUtility.readFile(kafkaProperties.getCustomerTopic().getFilePath(),
				kafkaProperties.getCustomerTopic().getAvroSchemaPath(), kafkaProperties.getCustomerTopic().getTopic(),
				this);

		FileUtility.readFile(kafkaProperties.getCardTopic().getFilePath(),
				kafkaProperties.getCardTopic().getAvroSchemaPath(), kafkaProperties.getCardTopic().getTopic(), this);

		FileUtility.readFile(kafkaProperties.getMerchantTopic().getFilePath(),
				kafkaProperties.getMerchantTopic().getAvroSchemaPath(), kafkaProperties.getMerchantTopic().getTopic(),
				this);

	}

	@Autowired
	@Qualifier("eventProducer")
	Producer<String, String> eventProducer;

	@Override
	public void dispatch(String event, String topic) {
		LOGGER.info("Event dispatch started topic = {} event = {} ", topic, event);
		ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, event);
		eventProducer.send(data, new EventCallBack());
	}

}
