package com.edureka.kafka.api;

public interface EventProducerApi {

	void dispatch(String event, String topic);

}
