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

import com.edureka.capstone.CardDetails;
import com.edureka.capstone.FileProperties;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.spark.streaming.Duration;

public class SparkStreamingCardJob {

	private final static Logger LOGGER = LoggerFactory.getLogger(SparkStreamingCardJob.class);

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
		conf.setAppName(SparkStreamingCustomerJob.class.getSimpleName());
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

	private static void saveToCassandra(String value, JavaSparkContext jsc) {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(FileProperties.CARD_AVRO);
		Injection<GenericRecord, String> recordInjection = GenericAvroCodecs.toJson(schema);
		GenericRecord record = recordInjection.invert(value).get();

		CardDetails cardDetails = new CardDetails(Long.parseLong(record.get("cardId").toString()),
				Long.parseLong(record.get("customerId").toString()), record.get("bank").toString(),
				record.get("type").toString(), Integer.parseInt(record.get("expiryMonth").toString()),
				Integer.parseInt(record.get("expiryYear").toString()), record.get("cardNum").toString(),
				Float.parseFloat(record.get("cardLimit").toString()),
				Integer.parseInt(record.get("valid_month").toString()),
				Integer.parseInt(record.get("valid_year").toString()));

		List<CardDetails> cardDetailList = Arrays.asList(cardDetails);

		LOGGER.error("cardDetailList List = {} jsc = {} ", cardDetailList, jsc);

		JavaRDD<CardDetails> newRDD = jsc.parallelize(cardDetailList);

		LOGGER.info("newRDD = {} ", newRDD);

		javaFunctions(newRDD).writerBuilder("capstone", "carddetails", mapToRow(CardDetails.class)).saveToCassandra();
	}

	public static void main(String[] args) throws InterruptedException {

		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));

		ssc = new JavaStreamingContext(jsc, new Duration(1000));

		Set<String> topics = Collections.singleton("card_topic");

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

	}

}
