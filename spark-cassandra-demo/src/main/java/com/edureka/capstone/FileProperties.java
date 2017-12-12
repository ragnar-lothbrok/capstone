package com.edureka.capstone;

import java.util.Properties;

public class FileProperties {

	public static Properties properties = null;
	
	static {
		properties = new Properties();
		properties.put("com.smcc.app.cassandra.host","localhost");
		properties.put("com.smcc.app.cassandra.port","9042");
		properties.put("localmode",true);
		properties.put("topxmerchants",10);
		
		properties.put("metadata.broker.list", "192.168.0.15:9092");
		properties.put("auto.offset.reset", "largest");
		properties.put("group.id", "groupid");
		properties.put("enable.auto.commit", "true");
	}
	
	public static final String CUSTOMER_AVRO = "{\n" + 
			"    \"type\": \"record\",\n" + 
			"    \"name\": \"customer\",\n" + 
			"    \"fields\": [\n" + 
			"        { \"name\": \"customerId\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"customerName\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"mobileNumber\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"gender\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"bithDate\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"email\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"address\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"state\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"country\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"pincode\", \"type\": \"int\" },\n" + 
			"        { \"name\": \"userType\", \"type\": \"int\" },\n" + 
			"        { \"name\": \"vehicleType\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"firstVisit\", \"type\": \"long\" }\n" + 
			"    ]\n" + 
			"}";
	
	public static final String CARD_AVRO= "{\n" + 
			"    \"type\": \"record\",\n" + 
			"    \"name\": \"card\",\n" + 
			"    \"fields\": [\n" + 
			"        { \"name\": \"cardId\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"customerId\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"bank\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"type\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"nameOnCard\", \"type\": \"string\" },\n" + 
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
			"        { \"name\": \"merchantName\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"mobileNumber\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"startDate\", \"type\": \"long\" },\n" + 
			"        { \"name\": \"email\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"address\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"state\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"country\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"pincode\", \"type\": \"int\" },\n" + 
			"        { \"name\": \"merchantType\", \"type\": \"int\" },\n" + 
			"        { \"name\": \"segment\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"taxRegNum\", \"type\": \"string\" },\n" + 
			"        { \"name\": \"description\", \"type\": \"string\" }\n" + 
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
			"        { \"name\": \"invoiceAmount\", \"type\": \"float\" }\n" + 
			"    ]\n" + 
			"}";
	
}
