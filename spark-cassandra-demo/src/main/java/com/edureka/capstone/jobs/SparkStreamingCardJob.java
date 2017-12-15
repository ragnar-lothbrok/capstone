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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.edureka.capstone.CardDetails;
import com.edureka.capstone.FileProperties;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.spark.streaming.Duration;

public class SparkStreamingCardJob {

	private static JavaStreamingContext ssc;

	static VoidFunction<Tuple2<String, String>> mapFunc = new VoidFunction<Tuple2<String, String>>() {

		private static final long serialVersionUID = 1L;

		@Override
		public void call(Tuple2<String, String> arg0) throws Exception {
			Schema.Parser parser = new Schema.Parser();
			Schema schema = parser.parse(FileProperties.CARD_AVRO);
			Injection<GenericRecord, String> recordInjection = GenericAvroCodecs.toJson(schema);
			GenericRecord record = recordInjection.invert(arg0._2).get();

			CardDetails cardDetails = new CardDetails(Long.parseLong(record.get("cardId").toString()),
					Long.parseLong(record.get("customerId").toString()), record.get("bank").toString(),
					record.get("type").toString(), record.get("nameOnCard").toString(),
					Integer.parseInt(record.get("expiryMonth").toString()),
					Integer.parseInt(record.get("expiryYear").toString()), record.get("cardNum").toString(),
					Float.parseFloat(record.get("cardLimit").toString()));

			List<CardDetails> cardDetailList = Arrays.asList(cardDetails);

			JavaRDD<CardDetails> newRDD = ssc.sparkContext().parallelize(cardDetailList);

			javaFunctions(newRDD).writerBuilder("capstone", "carddetails", mapToRow(CardDetails.class))
					.saveToCassandra();
		}
	};

	public static void main(String[] args) throws InterruptedException {

		Properties prop = FileProperties.properties;
		
		SparkConf conf  = null;
		if(Boolean.parseBoolean(prop.get("localmode").toString())) {
			conf = new SparkConf().setMaster("local[*]");
		}else {
			conf = new SparkConf();
		}
		conf.setAppName(BankMerchantAggregationJob.class.getName());
		conf.set("spark.cassandra.connection.host", prop.get("com.smcc.app.cassandra.host").toString());
		conf.setAppName(SparkStreamingCardJob.class.getName());
		conf.set("hadoop.home.dir", "/");

		ssc = new JavaStreamingContext(conf, new Duration(2000));

		Map<String, String> kafkaParams = new HashMap<>();

		kafkaParams.put("metadata.broker.list", prop.get("metadata.broker.list").toString());
		kafkaParams.put("auto.offset.reset", prop.get("auto.offset.reset").toString());
		kafkaParams.put("group.id", prop.get("group.id").toString());
		kafkaParams.put("enable.auto.commit", prop.get("enable.auto.commit").toString());
		
		Set<String> topics = Collections.singleton("card_topic");

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

	}

}
