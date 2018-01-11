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
import java.util.function.Consumer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.edureka.capstone.BankMerchantTransaction;
import com.edureka.capstone.CustomerTransactionCounter;
import com.edureka.capstone.DailyTransactionCounter;
import com.edureka.capstone.FileProperties;
import com.edureka.capstone.MerchantGenderSegmentTransaction;
import com.edureka.capstone.MerchantGenderTransaction;
import com.edureka.capstone.MerchantTransactionCounter;
import com.edureka.capstone.OrderTransaction;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.spark.streaming.Duration;

public class SparkStreamingTransactionJob {

	private final static Logger LOGGER = LoggerFactory.getLogger(SparkStreamingTransactionJob.class);

	private static SparkConf conf = null;

	private static JavaStreamingContext ssc = null;

	private static Map<String, String> kafkaParams = new HashMap<>();

	static {
		Properties prop = FileProperties.properties;
		if (Boolean.parseBoolean(prop.get("localmode").toString())) {
			conf = new SparkConf().setMaster("local[*]");
		} else {
			conf = new SparkConf();
		}
		conf.setAppName(SparkStreamingTransactionJob.class.getSimpleName());
		conf.set("spark.cassandra.connection.host", prop.get("com.smcc.app.cassandra.host").toString());
		if (prop.get("spark.cassandra.auth.username") != null) {
			conf.set("spark.cassandra.auth.username", prop.get("spark.cassandra.auth.username").toString());
			conf.set("spark.cassandra.auth.password", prop.get("spark.cassandra.auth.password").toString());
		} else {
			conf.set("hadoop.home.dir", "/");
		}
		conf.setAppName(SparkStreamingTransactionJob.class.getName());

		kafkaParams.put("metadata.broker.list", prop.get("metadata.broker.list").toString());
		kafkaParams.put("auto.offset.reset", prop.get("auto.offset.reset").toString());
		kafkaParams.put("group.id", prop.get("group.id").toString());
		kafkaParams.put("enable.auto.commit", prop.get("enable.auto.commit").toString());

	}
	static SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy");

	private static void saveToCassandra(String value, JavaSparkContext jsc) {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(FileProperties.TRANSACTION_AVRO);
		Injection<GenericRecord, String> recordInjection = GenericAvroCodecs.toJson(schema);
		GenericRecord record = recordInjection.invert(value).get();

		OrderTransaction transaction = new OrderTransaction(record.get("txId").toString(),
				Long.parseLong(record.get("customerId").toString()),
				Long.parseLong(record.get("merchantId").toString()), record.get("status").toString(),
				Long.parseLong(record.get("timestamp").toString()), record.get("invoiceNum").toString(),
				Float.parseFloat(record.get("invoiceAmount").toString()), record.get("segment").toString());

		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(transaction.getTimestamp());

		LOGGER.info("transaction = {} ", transaction);

		CassandraTableScanJavaRDD<CassandraRow> txDetails = javaFunctions(jsc)
				.cassandraTable("capstone", "order_transaction")
				.where("transactionid = '" + transaction.getTransactionid() + "'");

		long count = txDetails.count();

		LOGGER.info("count = {} ", count);

		if (count <= 0) {
			List<OrderTransaction> transactionList = Arrays.asList(transaction);

			LOGGER.info("transactionList = {} ", transactionList);

			JavaRDD<OrderTransaction> newRDD = jsc.parallelize(transactionList);

			javaFunctions(newRDD).writerBuilder("capstone", "order_transaction", mapToRow(OrderTransaction.class))
					.saveToCassandra();

			CassandraTableScanJavaRDD<CassandraRow> customerDetails = javaFunctions(jsc)
					.cassandraTable("capstone", "bank_by_customer")
					.where("customerid = " + transaction.getCustomerid());
			String bank = null;
			if (customerDetails.count() > 0) {
				bank = customerDetails.first().getString("bank");
			}

			CustomerTransactionCounter customerTransactionCounter = new CustomerTransactionCounter();
			MerchantTransactionCounter merchantTransactionCounter = new MerchantTransactionCounter();
			DailyTransactionCounter dailyTransactionCounter = new DailyTransactionCounter();

			customerTransactionCounter.setCustomerid(Long.parseLong(record.get("customerId").toString()));
			merchantTransactionCounter.setMerchantid(Long.parseLong(record.get("merchantId").toString()));
			dailyTransactionCounter.setDate(sdf.format(cal.getTime()));

			if (transaction.getStatus().equalsIgnoreCase("SUCCESS")) {
				customerTransactionCounter.setOrdersuccesscounter(1l);
				merchantTransactionCounter.setOrdersuccesscounter(1l);
				dailyTransactionCounter.setOrdersuccesscounter(1l);
			} else {
				customerTransactionCounter.setOrdercancelcounter(1l);
				merchantTransactionCounter.setOrdercancelcounter(1l);
				dailyTransactionCounter.setOrdercancelcounter(1l);
			}

			if (transaction.getInvoiceamount() <= 500) {
				customerTransactionCounter.setOrderbelow500(1l);
				merchantTransactionCounter.setOrderbelow500(1l);
				dailyTransactionCounter.setOrderbelow500(1l);
			} else if (transaction.getInvoiceamount() > 500 && transaction.getInvoiceamount() <= 1000) {
				customerTransactionCounter.setOrderbelow1000(1l);
				merchantTransactionCounter.setOrderbelow1000(1l);
				dailyTransactionCounter.setOrderbelow1000(1l);
			} else if (transaction.getInvoiceamount() > 1000 && transaction.getInvoiceamount() < 2000) {
				customerTransactionCounter.setOrderbelow2000(1l);
				merchantTransactionCounter.setOrderbelow2000(1l);
				dailyTransactionCounter.setOrderbelow2000(1l);
			} else {
				customerTransactionCounter.setOrderabove2000(1l);
				merchantTransactionCounter.setOrderabove2000(1l);
				dailyTransactionCounter.setOrderabove2000(1l);
			}

			BankMerchantTransaction bankMerchantTransaction = new BankMerchantTransaction(bank,
					transaction.getMerchantid(), transaction.getInvoiceamount(), 0l, transaction.getSegment(),
					cal.get(Calendar.YEAR), cal.get(Calendar.MONTH));

			CassandraTableScanJavaRDD<CassandraRow> bankMerchantDetails = javaFunctions(jsc)
					.cassandraTable("capstone", "bank_merchant_transaction")
					.where("year=" + cal.get(Calendar.YEAR) + " and month=" + cal.get(Calendar.MONTH) + " and bank = '"
							+ bankMerchantTransaction.getBank() + "' and merchantid="
							+ bankMerchantTransaction.getMerchantid() + " and segment='"
							+ bankMerchantTransaction.getSegment() + "'");
			float amount = 0f;
			Long orderCount = 0l;

			if (bankMerchantDetails.count() > 0) {
				amount = bankMerchantDetails.first().getFloat("totalamount");
				orderCount = bankMerchantDetails.first().getLong("ordercount");
			}

			bankMerchantTransaction.setTotalamount(amount + bankMerchantTransaction.getTotalamount());
			bankMerchantTransaction.setOrdercount(orderCount + 1);

			javaFunctions(jsc.parallelize(Arrays.asList(bankMerchantTransaction)))
					.writerBuilder("capstone", "bank_merchant_transaction", mapToRow(BankMerchantTransaction.class))
					.saveToCassandra();

			javaFunctions(jsc.parallelize(Arrays.asList(customerTransactionCounter)))
					.writerBuilder("capstone", "customer_transaction", mapToRow(CustomerTransactionCounter.class))
					.saveToCassandra();

			javaFunctions(jsc.parallelize(Arrays.asList(merchantTransactionCounter)))
					.writerBuilder("capstone", "merchant_transaction", mapToRow(MerchantTransactionCounter.class))
					.saveToCassandra();

			javaFunctions(jsc.parallelize(Arrays.asList(dailyTransactionCounter)))
					.writerBuilder("capstone", "daily_transaction", mapToRow(DailyTransactionCounter.class))
					.saveToCassandra();

			CassandraTableScanJavaRDD<CassandraRow> customerGenderDetails = javaFunctions(jsc)
					.cassandraTable("capstone", "customer").where("customerid = " + transaction.getCustomerid());
			String gender = null;
			if (customerGenderDetails.count() > 0) {
				gender = customerGenderDetails.first().getString("gender");
			}

			MerchantGenderTransaction merchantGenderTransaction = new MerchantGenderTransaction(
					bankMerchantTransaction.getYear(), bankMerchantTransaction.getMonth(),
					bankMerchantTransaction.getMerchantid(), transaction.getInvoiceamount(), gender);

			CassandraTableScanJavaRDD<CassandraRow> merchantCustomerGenderDetails = javaFunctions(jsc)
					.cassandraTable("capstone", "merchant_gender_transaction")
					.where("year=" + cal.get(Calendar.YEAR) + " and month=" + cal.get(Calendar.MONTH)
							+ " and merchantid=" + bankMerchantTransaction.getMerchantid() + " and gender='" + gender
							+ "'");
			amount = 0f;
			if (merchantCustomerGenderDetails.count() > 0) {
				amount = merchantCustomerGenderDetails.first().getFloat("amount");
			}
			merchantGenderTransaction.setAmount(amount + merchantGenderTransaction.getAmount());

			javaFunctions(jsc.parallelize(Arrays.asList(merchantGenderTransaction)))
					.writerBuilder("capstone", "merchant_gender_transaction", mapToRow(MerchantGenderTransaction.class))
					.saveToCassandra();

			MerchantGenderSegmentTransaction merchantGenderSegmentTransaction = new MerchantGenderSegmentTransaction(
					bankMerchantTransaction.getYear(), bankMerchantTransaction.getMonth(),
					bankMerchantTransaction.getMerchantid(), transaction.getInvoiceamount(), gender,
					transaction.getSegment());

			CassandraTableScanJavaRDD<CassandraRow> merchantGenderSegmentTransactionDetails = javaFunctions(jsc)
					.cassandraTable("capstone", "merchant_gender_segment_transaction")
					.where("year=" + cal.get(Calendar.YEAR) + " and month=" + cal.get(Calendar.MONTH)
							+ " and merchantid=" + merchantGenderSegmentTransaction.getMerchantid() + " and gender='"
							+ gender + "' and segment='" + merchantGenderSegmentTransaction.getSegment() + "'");
			amount = 0f;
			if (merchantGenderSegmentTransactionDetails.count() > 0) {
				amount = merchantGenderSegmentTransactionDetails.first().getFloat("amount");
			}
			merchantGenderSegmentTransaction.setAmount(amount + merchantGenderSegmentTransaction.getAmount());

			javaFunctions(jsc.parallelize(Arrays.asList(merchantGenderSegmentTransaction))).writerBuilder("capstone",
					"merchant_gender_segment_transaction", mapToRow(MerchantGenderSegmentTransaction.class))
					.saveToCassandra();
		}
	}

	public static void main(String[] args) throws InterruptedException {

		try {

			JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));

			ssc = new JavaStreamingContext(jsc, new Duration(1000));

			Set<String> topics = Collections.singleton("transaction_topic");

			JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
					String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

			JavaDStream<String> valuesStream = directKafkaStream.map(new Function<Tuple2<String, String>, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public String call(Tuple2<String, String> v1) throws Exception {
					return v1._2();
				}
			});

			valuesStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void call(JavaRDD<String> t) throws Exception {
					List<String> records = t.collect();
					LOGGER.info("Total records = {} ", records.size());
					records.iterator().forEachRemaining(new Consumer<String>() {

						@Override
						public void accept(String t) {
							saveToCassandra(t, jsc);
						}
					});
				}
			});

			ssc.start();
			ssc.awaitTermination();
		} catch (Exception e) {
			LOGGER.error("Exception occured while parsing = {} ", e.getMessage());
			throw e;
		}

	}

}
