package com.edureka.capstone.jobs;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.edureka.capstone.FileProperties;
import com.edureka.capstone.Merchant;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.spark.streaming.Duration;

public class SparkStreamingMerchantJob {

	private final static Logger LOGGER = LoggerFactory.getLogger(SparkStreamingMerchantJob.class);

	private static SparkConf conf = null;

	private static Map<String, String> kafkaParams = new HashMap<>();

	static {
		Properties prop = FileProperties.properties;
		if (Boolean.parseBoolean(prop.get("localmode").toString())) {
			conf = new SparkConf().setMaster("local[*]");
		} else {
			conf = new SparkConf();
		}
		conf.setAppName(SparkStreamingCustomerJob.class.getName());
		conf.set("spark.cassandra.connection.host", prop.get("com.smcc.app.cassandra.host").toString());
		if (prop.get("spark.cassandra.auth.username") != null) {
			conf.set("spark.cassandra.auth.username", prop.get("spark.cassandra.auth.username").toString());
			conf.set("spark.cassandra.auth.password", prop.get("spark.cassandra.auth.password").toString());
		} else {
			conf.set("hadoop.home.dir", "/");
		}
		conf.setAppName(SparkStreamingCardJob.class.getName());

		kafkaParams.put("metadata.broker.list", prop.get("metadata.broker.list").toString());
		kafkaParams.put("auto.offset.reset", prop.get("auto.offset.reset").toString());
		kafkaParams.put("group.id", prop.get("group.id").toString());
		kafkaParams.put("enable.auto.commit", prop.get("enable.auto.commit").toString());

	}

	static VoidFunction<Tuple2<String, String>> mapFunc = new VoidFunction<Tuple2<String, String>>() {

		private static final long serialVersionUID = 1L;

		@Override
		public void call(Tuple2<String, String> arg0) throws Exception {

			try {
				Schema.Parser parser = new Schema.Parser();
				Schema schema = parser.parse(FileProperties.MERCHANT_AVRO);
				Injection<GenericRecord, String> recordInjection = GenericAvroCodecs.toJson(schema);
				GenericRecord record = recordInjection.invert(arg0._2).get();

				Merchant merchant = new Merchant(Long.parseLong(record.get("merchantId").toString()),
						record.get("merchantName").toString(), record.get("email").toString(),
						record.get("address").toString(), record.get("state").toString(),
						record.get("country").toString(), Long.parseLong(record.get("pincode").toString()),
						record.get("segment").toString(), record.get("taxRegNum").toString(),
						record.get("description").toString(), Long.parseLong(record.get("startDate").toString()),
						Integer.parseInt(record.get("merchantType").toString()), record.get("mobileNumber").toString());

				LOGGER.info("merchant = {} ", merchant);

				List<Merchant> merchantList = Arrays.asList(merchant);

				LOGGER.info("merchantList = " + merchantList);

				JavaRDD<Merchant> newRDD = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate())
						.parallelize(merchantList);

				javaFunctions(newRDD).writerBuilder("capstone", "merchant", mapToRow(Merchant.class)).saveToCassandra();

				LOGGER.info("INSERTED");
			} catch (Exception e) {
				LOGGER.error("Exception occured while parsing = {} ", e.getMessage());
				throw e;
			}
		}
	};

	public static void main(String[] args) throws Exception {
		try {

			JavaStreamingContext ssc = new JavaStreamingContext(
					JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf)), new Duration(2000));

			Set<String> topics = Collections.singleton("merchant_topic");

			JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
					String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

			VoidFunction<JavaPairRDD<String, String>> iterateFunc = new VoidFunction<JavaPairRDD<String, String>>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void call(JavaPairRDD<String, String> arg0) throws Exception {
					if (!arg0.isEmpty())
						arg0.foreach(mapFunc);
				}
			};

			directKafkaStream.foreachRDD(iterateFunc);

			ssc.start();
			ssc.awaitTermination();
		} catch (Exception e) {
			LOGGER.error("Exception occured while parsing = {} ", e.getMessage());
			throw e;
		}

	}

}
