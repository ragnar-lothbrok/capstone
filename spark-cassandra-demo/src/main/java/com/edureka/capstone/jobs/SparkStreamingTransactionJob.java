package com.edureka.capstone.jobs;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.edureka.capstone.CustomerTransactionCounter;
import com.edureka.capstone.DailyTransactionCounter;
import com.edureka.capstone.MerchantTransactionCounter;
import com.edureka.capstone.Transaction;
import com.edureka.capstone.helper.AvroSchemaDefinitionLoader;
import com.edureka.capstone.helper.PropertyFileReader;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.spark.streaming.Duration;

public class SparkStreamingTransactionJob {

	private static final Logger logger = Logger.getLogger(SparkStreamingTransactionJob.class);

	private static JavaStreamingContext ssc;
	
	static SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy");

	static VoidFunction<Tuple2<String, String>> mapFunc = new VoidFunction<Tuple2<String, String>>() {

		private static final long serialVersionUID = 1L;

		@Override
		public void call(Tuple2<String, String> arg0) throws Exception {
			Schema.Parser parser = new Schema.Parser();
			Schema schema = parser.parse(AvroSchemaDefinitionLoader.fromFile("schema/transaction.avro").get());
			Injection<GenericRecord, String> recordInjection = GenericAvroCodecs.toJson(schema);
			GenericRecord record = recordInjection.invert(arg0._2).get();

			Transaction transaction = new Transaction(record.get("txId").toString(),
					Long.parseLong(record.get("customerId").toString()),
					Long.parseLong(record.get("merchantId").toString()), record.get("status").toString(),
					Long.parseLong(record.get("timestamp").toString()), record.get("invoiceNum").toString(),
					Float.parseFloat(record.get("invoiceAmount").toString()));
			
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(transaction.getTimestamp());
			
			List<Transaction> transactionDetailList = Arrays.asList(transaction);

			JavaRDD<Transaction> newRDD = ssc.sparkContext().parallelize(transactionDetailList);

			javaFunctions(newRDD).writerBuilder("capstone", "transaction", mapToRow(Transaction.class))
					.saveToCassandra();
			
			CustomerTransactionCounter customerTransactionCounter = new CustomerTransactionCounter();
			MerchantTransactionCounter merchantTransactionCounter = new MerchantTransactionCounter();
			DailyTransactionCounter dailyTransactionCounter = new DailyTransactionCounter();
			
			customerTransactionCounter.setCustomerid(Long.parseLong(record.get("customerId").toString()));
			merchantTransactionCounter.setMerchantid(Long.parseLong(record.get("merchantId").toString()));
			dailyTransactionCounter.setDate(sdf.format(cal.getTime()));
			
			if(transaction.getStatus().equalsIgnoreCase("SUCCESS")) {
				customerTransactionCounter.setOrdersuccesscounter(1l);
				merchantTransactionCounter.setOrdersuccesscounter(1l);
				dailyTransactionCounter.setOrdersuccesscounter(1l);
			}else {
				customerTransactionCounter.setOrdercancelcounter(1l);
				merchantTransactionCounter.setOrdercancelcounter(1l);
				dailyTransactionCounter.setOrdercancelcounter(1l);
			}
			
			if(transaction.getInvoiceamount() <= 500) {
				customerTransactionCounter.setOrderbelow500(1l);
				merchantTransactionCounter.setOrderbelow500(1l);
				dailyTransactionCounter.setOrderbelow500(1l);
			}else if(transaction.getInvoiceamount() > 500 && transaction.getInvoiceamount() <= 1000) {
				customerTransactionCounter.setOrderbelow1000(1l);
				merchantTransactionCounter.setOrderbelow1000(1l);
				dailyTransactionCounter.setOrderbelow1000(1l);
			}else if(transaction.getInvoiceamount() > 1000 && transaction.getInvoiceamount() < 2000) {
				customerTransactionCounter.setOrderbelow2000(1l);
				merchantTransactionCounter.setOrderbelow2000(1l);
				dailyTransactionCounter.setOrderbelow2000(1l);
			}else {
				customerTransactionCounter.setOrderabove2000(1l);
				merchantTransactionCounter.setOrderabove2000(1l);
				dailyTransactionCounter.setOrderabove2000(1l);
			}

			List<CustomerTransactionCounter> customerTransactionCounterList = Arrays.asList(customerTransactionCounter);

			JavaRDD<CustomerTransactionCounter> customerTxRDD = ssc.sparkContext()
					.parallelize(customerTransactionCounterList);

			javaFunctions(customerTxRDD)
					.writerBuilder("capstone", "customer_transaction", mapToRow(CustomerTransactionCounter.class))
					.saveToCassandra();
			
			
			List<MerchantTransactionCounter> merchantTransactionCounterList = Arrays.asList(merchantTransactionCounter);

			JavaRDD<MerchantTransactionCounter> merchantTxRDD = ssc.sparkContext()
					.parallelize(merchantTransactionCounterList);

			javaFunctions(merchantTxRDD)
					.writerBuilder("capstone", "merchant_transaction", mapToRow(MerchantTransactionCounter.class))
					.saveToCassandra();
			
			List<DailyTransactionCounter> dailyTransactionCounterList = Arrays.asList(dailyTransactionCounter);

			JavaRDD<DailyTransactionCounter> dailyTxRDD = ssc.sparkContext()
					.parallelize(dailyTransactionCounterList);

			javaFunctions(dailyTxRDD)
					.writerBuilder("capstone", "daily_transaction", mapToRow(DailyTransactionCounter.class))
					.saveToCassandra();
		}
	};

	public static void main(String[] args) throws InterruptedException {

		Properties prop = new Properties();
		try {
			prop = PropertyFileReader.readPropertyFile();
		} catch (Exception e1) {
			logger.error(e1.getMessage());
		}

		SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
		ssc = new JavaStreamingContext(conf, new Duration(2000));

		Map<String, String> kafkaParams = new HashMap<>();

		kafkaParams.put("metadata.broker.list", "192.168.0.15:9092");
		kafkaParams.put("auto.offset.reset", "smallest");
		Set<String> topics = Collections.singleton("transaction_topic");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		conf.setAppName(prop.getProperty("com.smcc.spark.app.name"));
		conf.setMaster("local[*]");
		conf.set("spark.cassandra.connection.host", prop.getProperty("com.smcc.app.cassandra.host"));
		conf.set("hadoop.home.dir", "/");

		VoidFunction<JavaPairRDD<String, String>> iterateFunc = new VoidFunction<JavaPairRDD<String, String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, String> arg0) throws Exception {
				arg0.foreach(mapFunc);
			}
		};

		directKafkaStream.foreachRDD(iterateFunc);

		ssc.start();
		ssc.awaitTermination();

	}

}
