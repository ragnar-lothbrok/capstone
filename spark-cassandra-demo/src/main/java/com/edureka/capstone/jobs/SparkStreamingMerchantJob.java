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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.edureka.capstone.FileProperties;
import com.edureka.capstone.Merchant;
import com.edureka.capstone.dtos.MerchantDto;
import com.google.gson.Gson;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.spark.streaming.Duration;

public class SparkStreamingMerchantJob {

	private static JavaStreamingContext ssc;
	
	private static Gson gson = new Gson();

	static VoidFunction<Tuple2<String, String>> mapFunc = new VoidFunction<Tuple2<String, String>>() {

		private static final long serialVersionUID = 1L;

		@Override
		public void call(Tuple2<String, String> arg0) throws Exception {

			
			MerchantDto merchantDto = gson.fromJson( arg0._2(), MerchantDto.class);
			
			System.out.println("MerchantDto = " + merchantDto);

			Merchant merchant = new Merchant(merchantDto.getMerchantId().longValue(),
					merchantDto.getMerchantName(), merchantDto.getEmail(),
					merchantDto.getAddress(), merchantDto.getState(), merchantDto.getCountry(),
					merchantDto.getPincode().longValue(), merchantDto.getSegment(),
					merchantDto.getTaxRegNum(), merchantDto.getDescription(),
					merchantDto.getStartDate().longValue(),
					merchantDto.getMerchantType(), merchantDto.getMobileNumber().toString());

			System.out.println("merchant = " + merchant);

			List<Merchant> merchantList = Arrays.asList(merchant);

			JavaRDD<Merchant> newRDD = ssc.sparkContext().parallelize(merchantList);

			javaFunctions(newRDD).writerBuilder("capstone", "merchant", mapToRow(Merchant.class)).saveToCassandra();
		}
	};

	public static void main(String[] args) throws InterruptedException {

		try {
			
			Properties prop = FileProperties.properties;

			SparkConf conf = null;
			if (Boolean.parseBoolean(prop.get("localmode").toString())) {
				conf = new SparkConf().setMaster("local[*]");
			} else {
				conf = new SparkConf();
			}
			conf.set("spark.cassandra.connection.host", prop.get("com.smcc.app.cassandra.host").toString());
			conf.setAppName(SparkStreamingMerchantJob.class.getName());

			if (prop.get("spark.cassandra.auth.username") != null) {
				conf.set("spark.cassandra.auth.username", prop.get("spark.cassandra.auth.username").toString());
				conf.set("spark.cassandra.auth.password", prop.get("spark.cassandra.auth.password").toString());
			} else {
				conf.set("hadoop.home.dir", "/");
			}

			ssc = new JavaStreamingContext(conf, new Duration(2000));

			Map<String, String> kafkaParams = new HashMap<>();

			kafkaParams.put("metadata.broker.list", prop.get("metadata.broker.list").toString());
			kafkaParams.put("auto.offset.reset", prop.get("auto.offset.reset").toString());
			kafkaParams.put("group.id", prop.get("group.id").toString());
			kafkaParams.put("enable.auto.commit", prop.get("enable.auto.commit").toString());
			kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			Set<String> topics = Collections.singleton("merchant_topic");

			JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
					String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

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

			System.out.println("STARTED=========");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw e;
		}

	}

}
