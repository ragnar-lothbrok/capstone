package com.edureka.capstone.jobs;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.text.SimpleDateFormat;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.edureka.capstone.FileProperties;
import com.edureka.capstone.Merchant;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.spark.streaming.Duration;

public class SparkStreamingMerchantJob {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(SparkStreamingMerchantJob.class);
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("mm/dd/yy");
	
	private static SimpleDateFormat sdf1 = new SimpleDateFormat("mm/dd/yyyy");

	private static JavaStreamingContext ssc;

	static VoidFunction<Tuple2<String, String>> mapFunc = new VoidFunction<Tuple2<String, String>>() {

		private static final long serialVersionUID = 1L;

		@Override
		public void call(Tuple2<String, String> arg0) throws Exception {

			try {
				System.out.println("tuple value details = " + arg0._2());
				String split[] = arg0._2().split(",");
				
				LOGGER.error("record = {} ",arg0._2());

				Merchant merchant = new Merchant(Long.parseLong(split[0]),
						split[1], split[4].trim(),
						split[5].trim(), split[6].trim(),
						split[7].trim(), Long.parseLong(split[8].trim()),
						split[10].trim(), split[11].trim(),
						split[12].trim(), sdf1.parse(split[3]).getTime(),
						Integer.parseInt(split[9].trim()), split[2].trim());

				LOGGER.error("merchant = {} " , merchant);

				List<Merchant> merchantList = Arrays.asList(merchant);
				
				LOGGER.error("merchantList = "+merchantList);

				if(merchantList != null & merchantList.size() > 0) {
					JavaRDD<Merchant> newRDD = ssc.sparkContext().parallelize(merchantList);
					
					if(!newRDD.isEmpty())
						javaFunctions(newRDD).writerBuilder("capstone", "merchant", mapToRow(Merchant.class)).saveToCassandra();
				}
			} catch (Exception e) {
				System.out.println("Exception occured while parsing  " + e.getMessage());
				LOGGER.error("Exception occured while parsing = {} " , e.getMessage());
				throw e;
			}
		}
	};

	public static void main(String[] args) throws Exception {
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
			conf.set("hadoop.home.dir", "/");
			if (prop.get("spark.cassandra.auth.username") != null) {
				conf.set("spark.cassandra.auth.username", prop.get("spark.cassandra.auth.username").toString());
				conf.set("spark.cassandra.auth.password", prop.get("spark.cassandra.auth.password").toString());
			}

			ssc = new JavaStreamingContext(conf, new Duration(2000));

			Map<String, String> kafkaParams = new HashMap<>();

			kafkaParams.put("metadata.broker.list", prop.get("metadata.broker.list").toString());
			kafkaParams.put("auto.offset.reset", prop.get("auto.offset.reset").toString());
			kafkaParams.put("group.id", prop.get("group.id").toString());
			kafkaParams.put("enable.auto.commit", prop.get("enable.auto.commit").toString());

			Set<String> topics = Collections.singleton("merchant_topic");

			JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
					String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

			VoidFunction<JavaPairRDD<String, String>> iterateFunc = new VoidFunction<JavaPairRDD<String, String>>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void call(JavaPairRDD<String, String> arg0) throws Exception {
					if(!arg0.isEmpty())
						arg0.foreach(mapFunc);
				}
			};

			directKafkaStream.foreachRDD(iterateFunc);

			ssc.start();
			ssc.awaitTermination();
		} catch (Exception e) {
			System.out.println("Exception occured while starting  " + e.getMessage());
			LOGGER.error("Exception occured while parsing = {} " , e.getMessage());
			throw e;
		}

	}

}
