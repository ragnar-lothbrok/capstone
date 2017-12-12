package com.edureka.capstone.jobs;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.edureka.capstone.FileProperties;
import com.google.common.collect.Lists;

import scala.Tuple2;
import scala.Tuple3;

public class BankMerchantAggregation {

	public static void main(String[] args) {

		NumberFormat formatter = new DecimalFormat("#0.00");
		
		Properties prop = FileProperties.properties;
		Integer topN = Integer.parseInt(prop.get("topxmerchants").toString());
		
		SparkConf conf  = null;
		if(Boolean.parseBoolean(prop.get("localmode").toString())) {
			conf = new SparkConf().setMaster("local[*]");
		}else {
			conf = new SparkConf();
		}
		conf.setAppName(BankMerchantAggregation.class.getName());
		conf.set("spark.cassandra.connection.host", prop.get("com.smcc.app.cassandra.host").toString());
		conf.set("hadoop.home.dir", "/");

		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(javaSparkContext);

		JavaRDD<CassandraRow> rdd = functions.cassandraTable("capstone", "bank_merchant_transaction")
				.where("date = '31-Jan-2012'");

		rdd.cache();

		JavaPairRDD<String, Float> pairRDD = rdd
				.mapToPair(r -> new Tuple2<String, Float>(r.getString("bank").toString(),
						Float.parseFloat(r.getFloat("totalamount").toString())));

		// Bank totalAmount Map
		JavaPairRDD<String, Float> reducePairRDD = pairRDD.reduceByKey(new Function2<Float, Float, Float>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Float call(Float v1, Float v2) throws Exception {
				return v1 + v2;
			}
		});

		Map<String, Float> bankTotalAmountMap = reducePairRDD.collectAsMap();

		JavaRDD<Tuple3<String, Long, Float>> bankMerchantPairRDD = rdd
				.map(r -> new Tuple3<String, Long, Float>(r.getString("bank").toString(), r.getLong("merchantid"),
						Float.parseFloat(r.getFloat("totalamount").toString())));

		JavaPairRDD<String, Iterable<Tuple3<String, Long, Float>>> bankMerchantPairRDDd = bankMerchantPairRDD
				.groupBy(new Function<Tuple3<String, Long, Float>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple3<String, Long, Float> v1) throws Exception {
						return v1._1();
					}
				});

		JavaRDD<Tuple3<String, String, Float>> finalAnswer = bankMerchantPairRDDd.flatMap(
				new FlatMapFunction<Tuple2<String, Iterable<Tuple3<String, Long, Float>>>, Tuple3<String, String, Float>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple3<String, String, Float>> call(
							Tuple2<String, Iterable<Tuple3<String, Long, Float>>> t) throws Exception {
						List<Tuple3<String, Long, Float>> list = Lists.newArrayList(t._2);
						Collections.sort(list, MyTupleComparator.INSTANCE);

						list = list.stream().limit(topN).collect(Collectors.toList());

						Float amount = 0.0f;

						System.out.println(list);

						for (Tuple3<String, Long, Float> tuple : list) {
							amount += tuple._3();
							System.out.println(tuple._3());
						}
						return Arrays.asList(new Tuple3<String, String, Float>(t._1,
								formatter.format(((amount * 100.0f) / bankTotalAmountMap.get(t._1))),
								bankTotalAmountMap.get(t._1))).iterator();
					}
				});

		System.out.println("Output = " + finalAnswer.collect());

	}

	static class MyTupleComparator implements Comparator<Tuple3<String, Long, Float>>, Serializable {

		private static final long serialVersionUID = 1L;
		final static MyTupleComparator INSTANCE = new MyTupleComparator();

		@Override
		public int compare(Tuple3<String, Long, Float> o1, Tuple3<String, Long, Float> o2) {
			return -o1._3().compareTo(o2._3());
		}

	}
}