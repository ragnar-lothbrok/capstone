package com.edureka.kafka.config.props;

import java.io.Serializable;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "producer")
public class KafkaProperties {

	private Topics cardTopic;
	private Topics merchantTopic;
	private Topics customerTopic;
	private Topics transactionTopic;

	private String bootstrap;
	private String brokers;
	// producer properties
	private String acks;
	private int retries;

	public Topics getCardTopic() {
		return cardTopic;
	}

	public void setCardTopic(Topics cardTopic) {
		this.cardTopic = cardTopic;
	}

	public Topics getMerchantTopic() {
		return merchantTopic;
	}

	public void setMerchantTopic(Topics merchantTopic) {
		this.merchantTopic = merchantTopic;
	}

	public Topics getCustomerTopic() {
		return customerTopic;
	}

	public void setCustomerTopic(Topics customerTopic) {
		this.customerTopic = customerTopic;
	}

	public Topics getTransactionTopic() {
		return transactionTopic;
	}

	public void setTransactionTopic(Topics transactionTopic) {
		this.transactionTopic = transactionTopic;
	}

	public String getAcks() {
		return acks;
	}

	public void setAcks(String acks) {
		this.acks = acks;
	}

	public int getRetries() {
		return retries;
	}

	public void setRetries(int retries) {
		this.retries = retries;
	}

	public String getBootstrap() {
		return bootstrap;
	}

	public void setBootstrap(String bootstrap) {
		this.bootstrap = bootstrap;
	}

	public String getBrokers() {
		return brokers;
	}

	public void setBrokers(String brokers) {
		this.brokers = brokers;
	}

	public static class Topics implements Serializable {

		private static final long serialVersionUID = 1L;

		private String topic;
		private Integer partitionCount;
		private String filePath;
		private String avroSchemaPath;

		public String getAvroSchemaPath() {
			return avroSchemaPath;
		}

		public void setAvroSchemaPath(String avroSchemaPath) {
			this.avroSchemaPath = avroSchemaPath;
		}

		public String getTopic() {
			return topic;
		}

		public void setTopic(String topic) {
			this.topic = topic;
		}

		public Integer getPartitionCount() {
			return partitionCount;
		}

		public void setPartitionCount(Integer partitionCount) {
			this.partitionCount = partitionCount;
		}

		public String getFilePath() {
			return filePath;
		}

		public void setFilePath(String filePath) {
			this.filePath = filePath;
		}

	}
}
