package com.edureka.capstone;

import java.util.Properties;

public class FileProperties {

	public static Properties properties = null;

	static {
		properties = new Properties();

		Boolean isEdurekaServer = true;
		if (isEdurekaServer) {
			properties.put("com.smcc.app.cassandra.host", "20.0.41.93");
			properties.put("com.smcc.app.cassandra.port", "9042");
			properties.put("spark.cassandra.auth.username", "edureka_339659");
			properties.put("spark.cassandra.auth.password", "edureka_3396594kw6i");
			properties.put("localmode", false);
			properties.put("metadata.broker.list", "20.0.31.78:9092,20.0.32.147:9092,20.0.31.127:9092");
//			properties.put("metadata.broker.list", "20.0.41.93:9092");
		} else {
			properties.put("com.smcc.app.cassandra.host", "localhost");
			properties.put("com.smcc.app.cassandra.port", "9042");
			properties.put("metadata.broker.list", "192.168.0.15:9092");
			properties.put("localmode", true);
		}

		properties.put("topxmerchants", 4);
		
		properties.put("auto.offset.reset", "largest");
//		properties.put("auto.offset.reset", "smallest");
		properties.put("group.id", "groupid");
		properties.put("enable.auto.commit", "true");

		properties.put("topmerchantcount", 5);
		properties.put("topSegmentCount", 5);

		properties.put("yearstats", 2012);
	}

	public static final String CUSTOMER_AVRO = "{\n" + 
			"    \"type\": \"record\",\n" + 
			"    \"name\": \"customer\",\n" + 
			"    \"fields\": [\n" + 
			"        { \"name\": \"customerId\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"customerName\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"mobileNumber\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"gender\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"title\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"martial_status\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"bithDate\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"email\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"address\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"state\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"country\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"pincode\", \"type\": \"int\" },\n" + 
			"        { \"name\": \"created\", \"type\": \"long\" }\n" + 
			"    ]\n" + 
			"}";

	public static final String CARD_AVRO = "{\n" + 
			"    \"type\": \"record\",\n" + 
			"    \"name\": \"card\",\n" + 
			"    \"fields\": [\n" + 
			"        { \"name\": \"cardId\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"customerId\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"bank\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"type\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"valid_month\", \"type\": \"int\" },\n" + 
			"        { \"name\": \"valid_year\", \"type\": \"int\" },\n" + 
			"        { \"name\": \"expiryMonth\", \"type\": \"int\" },\n" + 
			"        { \"name\": \"expiryYear\", \"type\": \"int\" },\n" + 
			"        { \"name\": \"cardNum\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"cardLimit\", \"type\": \"float\" }\n" + 
			"    ]\n" + 
			"}";

	public static final String MERCHANT_AVRO = "{\n" + 
			"    \"type\": \"record\",\n" + 
			"    \"name\": \"merchant\",\n" + 
			"    \"fields\": [\n" + 
			"        { \"name\": \"merchantId\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"taxRegNum\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"merchantName\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"mobileNumber\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"startDate\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"email\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"address\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"state\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"country\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"pincode\", \"type\": \"int\" },\n" + 
			"        { \"name\": \"description\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"longlat\", \"type\": \"string\" }\n" + 
			"    ]\n" + 
			"}";

	public static final String TRANSACTION_AVRO = "{\n" + 
			"    \"type\": \"record\",\n" + 
			"    \"name\": \"transaction\",\n" + 
			"    \"fields\": [\n" + 
			"        { \"name\": \"txId\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"customerId\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"merchantId\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"status\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"timestamp\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"invoiceNum\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"invoiceAmount\", \"type\": \"float\" },\n" + 
			"        { \"name\": \"segment\", \"type\": \"string\" }\n" + 
			"    ]\n" + 
			"}";

}
