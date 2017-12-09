package com.edureka.kafka.service;

import java.util.List;

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
import com.edureka.kafka.performance.MonitoringCache;
import com.edureka.kafka.performance.MonitoringCache.Caches;
import com.edureka.kafka.producer.EventCallBack;
import com.edureka.kafka.utility.FileUtility;

@Service
public class EventProducerImpl implements EventProducerApi {

	@Autowired
	private KafkaProperties kafkaProperties;

	private static final Logger LOGGER = LoggerFactory.getLogger(EventProducerImpl.class);

	@PostConstruct
	private void test() {
		List<String> events = FileUtility.readFile(kafkaProperties.getTransactionTopic().getFilePath(),
				kafkaProperties.getTransactionTopic().getAvroSchemaPath());
		for (String event : events) {
			dispatch(event, kafkaProperties.getTransactionTopic().getTopic());
		}
		
		events = FileUtility.readFile(kafkaProperties.getCustomerTopic().getFilePath(),
				kafkaProperties.getCustomerTopic().getAvroSchemaPath());
		for (String event : events) {
			dispatch(event, kafkaProperties.getCustomerTopic().getTopic());
		}
		
		events = FileUtility.readFile(kafkaProperties.getCardTopic().getFilePath(),
				kafkaProperties.getCardTopic().getAvroSchemaPath());
		for (String event : events) {
			dispatch(event, kafkaProperties.getCardTopic().getTopic());
		}
		
		events = FileUtility.readFile(kafkaProperties.getMerchantTopic().getFilePath(),
				kafkaProperties.getMerchantTopic().getAvroSchemaPath());
		for (String event : events) {
			dispatch(event, kafkaProperties.getMerchantTopic().getTopic());
		}
	}

	@Autowired
	@Qualifier("eventProducer")
	Producer<String, String> eventProducer;

	@Override
	public void dispatch(String event, String topic) {
		long startTime = System.currentTimeMillis();
		LOGGER.info("Event dispatch started = {} ", event);
		ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, event);
		eventProducer.send(data, new EventCallBack());
		MonitoringCache.updateStats(Caches.PRODUCT_EVENT, (System.currentTimeMillis() - startTime), 1);
	}

}
