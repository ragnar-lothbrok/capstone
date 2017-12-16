package com.edureka.capstone.jobs;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.edureka.capstone.FileProperties;
import com.google.common.collect.Lists;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import scala.Tuple6;

public class BankMerchantAggregationJob {

	private static JavaSparkContext javaSparkContext = null;

	public static void main(String[] args) throws ParseException {

		NumberFormat formatter = new DecimalFormat("#0.00");

		Properties prop = FileProperties.properties;
		Integer topN = Integer.parseInt(prop.get("topxmerchants").toString());

		SparkConf conf = null;
		if (Boolean.parseBoolean(prop.get("localmode").toString())) {
			conf = new SparkConf().setMaster("local[*]");
		} else {
			conf = new SparkConf();
		}
		conf.setAppName(BankMerchantAggregationJob.class.getName());
		conf.set("spark.cassandra.connection.host", prop.get("com.smcc.app.cassandra.host").toString());
		if(prop.get("spark.cassandra.auth.username") != null) {
			conf.set("spark.cassandra.auth.username", prop.get("spark.cassandra.auth.username").toString());
			conf.set("spark.cassandra.auth.password", prop.get("spark.cassandra.auth.password").toString());
		}
		conf.set("hadoop.home.dir", "/");

		javaSparkContext = new JavaSparkContext(conf);

		SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(javaSparkContext);

		Integer year = Integer.parseInt(prop.get("yearstats").toString());

		JavaRDD<CassandraRow> rdd = null;
		JavaRDD<CassandraRow> genderRdd = null;
		String months = "";
		for (int i = 0; i < 12; i++) {
			if (i == 11) {
				months = months + i;
				break;
			}
			months = months + i + ",";
		}

		rdd = functions.cassandraTable("capstone", "bank_merchant_transaction")
				.where("year = " + year + " and month in (" + months + ")");

		genderRdd = functions.cassandraTable("capstone", "merchant_gender_transaction")
				.where("year = " + year + " and month in (" + months + ")");

		rdd.cache();
		genderRdd.cache();

		JavaPairRDD<String, Tuple2<Float, Long>> pairRDD = rdd.mapToPair(
				r -> new Tuple2<String, Tuple2<Float, Long>>(r.getString("bank").toString(), new Tuple2<Float, Long>(
						Float.parseFloat(r.getFloat("totalamount").toString()), r.getLong("ordercount"))));

		// Bank totalAmount Map
		JavaPairRDD<String, Tuple2<Float, Long>> reducePairRDD = pairRDD
				.reduceByKey(new Function2<Tuple2<Float, Long>, Tuple2<Float, Long>, Tuple2<Float, Long>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Float, Long> call(Tuple2<Float, Long> v1, Tuple2<Float, Long> v2) throws Exception {
						return new Tuple2<Float, Long>(v1._1 + v2._1(), v1._2 + v2._2());
					}
				});

		Map<String, Tuple2<Float, Long>> bankTotalAmountMap = reducePairRDD.collectAsMap();

		List<Entry<String, Tuple2<Float, Long>>> list = new ArrayList<>(bankTotalAmountMap.entrySet());

		Collections.sort(list, Comparator2.INSTANCE);

		List<String> topMerchantByTransaction = list.stream().limit(5)
				.map(new java.util.function.Function<Entry<String, Tuple2<Float, Long>>, String>() {

					@Override
					public String apply(Entry<String, Tuple2<Float, Long>> t) {
						return t.getKey();
					}

				}).collect(Collectors.toList());

		System.out.println("TOP 5 Banks by Transaction => " + topMerchantByTransaction);

		JavaPairRDD<Long, Float> merchantTxAmountMap = rdd
				.mapToPair(r -> new Tuple2<Long, Float>(r.getLong("merchantid"),
						Float.parseFloat(r.getFloat("totalamount").toString())));

		JavaPairRDD<Long, Float> merchantTxAmountRDD = merchantTxAmountMap
				.reduceByKey(new Function2<Float, Float, Float>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Float call(Float v1, Float v2) throws Exception {
						return v1 + v2;
					}
				});

		Map<Long, Float> merchanttxAmountMap = merchantTxAmountRDD.collectAsMap();

		List<Entry<Long, Float>> merchantAmountList = new ArrayList<>(merchanttxAmountMap.entrySet());

		Collections.sort(merchantAmountList, Comparator3.INSTANCE);

		List<Long> merchantNameList = merchantAmountList.stream()
				.map(new java.util.function.Function<Entry<Long, Float>, Long>() {
					@Override
					public Long apply(Entry<Long, Float> t) {
						return t.getKey();
					}
				}).limit(Integer.parseInt(prop.get("topmerchantcount").toString())).collect(Collectors.toList());

		System.out.println("TOP 5 Merchant by Transaction Amount => " + merchantNameList);

		// Quarterly transaction amount of top 5 merchants

		JavaRDD<Tuple6<String, Long, Float, Long, String, String>> bankMerchantPairRDD = rdd
				.map(r -> new Tuple6<String, Long, Float, Long, String, String>(r.getString("bank").toString(),
						r.getLong("merchantid"), Float.parseFloat(r.getFloat("totalamount").toString()),
						r.getLong("ordercount"), (r.getInt("month") + 1) + "", r.getString("segment")));

		JavaPairRDD<String, Iterable<Tuple6<String, Long, Float, Long, String, String>>> bankMerchantPairRDDd = bankMerchantPairRDD
				.groupBy(new Function<Tuple6<String, Long, Float, Long, String, String>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple6<String, Long, Float, Long, String, String> v1) throws Exception {
						return v1._1();
					}
				});

		bankMerchantPairRDDd.cache();

		topMerchantTxAmountQuarterly(merchantNameList, bankMerchantPairRDD);

		topMerchantSegmentTxAmountMonthly(merchantNameList, bankMerchantPairRDD,
				Integer.parseInt(prop.get("topSegmentCount").toString()));

		topBanksTopMerchantTxAmount(formatter, topN, bankTotalAmountMap, bankMerchantPairRDDd);

		topMerchantCustomerTxAmountByGender(merchantNameList, genderRdd);

	}

	private static void topMerchantCustomerTxAmountByGender(List<Long> merchantNameList,
			JavaRDD<CassandraRow> genderRdd) {
		List<Tuple2<Integer, Tuple4<Long, Integer,String, Float>>> genderWiseTx = genderRdd.mapToPair(new PairFunction<CassandraRow, Integer, Tuple4<Long, Integer,String, Float>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Tuple4<Long, Integer,String, Float>> call(CassandraRow t) throws Exception {
				return new Tuple2<Integer, Tuple4<Long, Integer,String, Float>>(t.getInt("year"), new Tuple4<Long, Integer,String, Float>(t.getLong("merchantid"), t.getInt("month"),t.getString("gender"), t.getFloat("amount")));
			}
		}).filter(new Function<Tuple2<Integer,Tuple4<Long, Integer,String, Float>>, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<Integer, Tuple4<Long, Integer,String, Float>> v1) throws Exception {
				return merchantNameList.contains(v1._2()._1().longValue());
			}
		}).collect();
		
		System.out.println("genderWiseTx == "+genderWiseTx);
		System.out.println();
	}

	// bank merchantid totalamount ordercount date segment
	private static void topMerchantSegmentTxAmountMonthly(List<Long> merchantNameList,
			JavaRDD<Tuple6<String, Long, Float, Long, String, String>> bankMerchantPairRDD,
			final Integer topSegmentCount) {

		JavaRDD<Tuple2<Long, List<Tuple2<String, List<Tuple2<String, Float>>>>>> merchantSegmentTxAmountMonthly = bankMerchantPairRDD
				.filter(new Function<Tuple6<String, Long, Float, Long, String, String>, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple6<String, Long, Float, Long, String, String> v1) throws Exception {
						return merchantNameList.indexOf(v1._2()) != -1;
					}
				})
				.map(new Function<Tuple6<String, Long, Float, Long, String, String>, Tuple4<Long, String, String, Float>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple4<Long, String, String, Float> call(
							Tuple6<String, Long, Float, Long, String, String> v1) throws Exception {

						return new Tuple4<Long, String, String, Float>(v1._2(), v1._6(), " Month " + v1._5(), v1._3());
					}
				}).groupBy(new Function<Tuple4<Long, String, String, Float>, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Tuple4<Long, String, String, Float> v1) throws Exception {
						return v1._1();
					}
				})
				.map(new Function<Tuple2<Long, Iterable<Tuple4<Long, String, String, Float>>>, Tuple2<Long, List<Tuple2<String, List<Tuple2<String, Float>>>>>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, List<Tuple2<String, List<Tuple2<String, Float>>>>> call(
							Tuple2<Long, Iterable<Tuple4<Long, String, String, Float>>> v1) throws Exception {

						List<Tuple4<Long, String, String, Float>> tuples = Lists.newArrayList(v1._2);

						List<String> topSegment = topSegmentForMerchant(topSegmentCount, tuples);

						List<Tuple2<String, List<Tuple2<String, Float>>>> segmentMonthTransactionList = javaSparkContext
								.parallelize(tuples)
								.filter(new Function<Tuple4<Long, String, String, Float>, Boolean>() {

									private static final long serialVersionUID = 1L;

									@Override
									public Boolean call(Tuple4<Long, String, String, Float> v1) throws Exception {
										return topSegment.contains(v1._2());
									}
								}).mapToPair(
										new PairFunction<Tuple4<Long, String, String, Float>, String, Tuple2<String, Float>>() {

											private static final long serialVersionUID = 1L;

											@Override
											public Tuple2<String, Tuple2<String, Float>> call(
													Tuple4<Long, String, String, Float> t) throws Exception {
												return new Tuple2<String, Tuple2<String, Float>>(t._2(),
														new Tuple2<String, Float>(t._3(), t._4()));
											}
										})
								.groupByKey().mapToPair(
										new PairFunction<Tuple2<String, Iterable<Tuple2<String, Float>>>, String, List<Tuple2<String, Float>>>() {

											private static final long serialVersionUID = 1L;

											@Override
											public Tuple2<String, List<Tuple2<String, Float>>> call(
													Tuple2<String, Iterable<Tuple2<String, Float>>> t)
													throws Exception {
												List<Tuple2<String, Float>> monthAmoutMap = javaSparkContext
														.parallelize(Lists.newArrayList(t._2())).mapToPair(
																new PairFunction<Tuple2<String, Float>, String, Float>() {

																	private static final long serialVersionUID = 1L;

																	@Override
																	public Tuple2<String, Float> call(
																			Tuple2<String, Float> t) throws Exception {
																		return t;
																	}
																})
														.reduceByKey(new Function2<Float, Float, Float>() {

															private static final long serialVersionUID = 1L;

															@Override
															public Float call(Float v1, Float v2) throws Exception {
																return v1 + v2;
															}
														}).collect();
												return new Tuple2<String, List<Tuple2<String, Float>>>(t._1,
														monthAmoutMap);
											}
										})
								.collect();
						return new Tuple2<Long, List<Tuple2<String, List<Tuple2<String, Float>>>>>(v1._1(),
								segmentMonthTransactionList);
					}
				});

		// Top merchant transaction amount quarterly.
		System.out.println("Top merchantSegmentTxAmountMonthly = " + merchantSegmentTxAmountMonthly.collect());
	}

	private static List<String> topSegmentForMerchant(final Integer topSegmentCount,
			List<Tuple4<Long, String, String, Float>> tuples) {
		Map<String, Float> segmentAmountMap = javaSparkContext.parallelize(tuples)
				.mapToPair(new PairFunction<Tuple4<Long, String, String, Float>, String, Float>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Float> call(Tuple4<Long, String, String, Float> t) throws Exception {
						return new Tuple2<String, Float>(t._2(), t._4());
					}

				}).reduceByKey(new Function2<Float, Float, Float>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Float call(Float v1, Float v2) throws Exception {
						return v1 + v2;
					}
				}).collectAsMap();

		List<Entry<String, Float>> segmentAmountList = new ArrayList<>(segmentAmountMap.entrySet());
		Collections.sort(segmentAmountList, Comparator4.INSTANCE);

		List<String> topSegment = segmentAmountList.stream()
				.map(new java.util.function.Function<Entry<String, Float>, String>() {
					@Override
					public String apply(Entry<String, Float> t) {
						return t.getKey();
					}
				}).limit(topSegmentCount).collect(Collectors.toList());

		System.out.println("Top segment=== " + topSegment);

		return topSegment;
	}

	private static void topBanksTopMerchantTxAmount(NumberFormat formatter, Integer topN,
			Map<String, Tuple2<Float, Long>> bankTotalAmountMap,
			JavaPairRDD<String, Iterable<Tuple6<String, Long, Float, Long, String, String>>> bankMerchantPairRDDd) {
		JavaRDD<Tuple5<String, String, Long, String, Long>> finalAnswer = bankMerchantPairRDDd.flatMap(
				new FlatMapFunction<Tuple2<String, Iterable<Tuple6<String, Long, Float, Long, String, String>>>, Tuple5<String, String, Long, String, Long>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple5<String, String, Long, String, Long>> call(
							Tuple2<String, Iterable<Tuple6<String, Long, Float, Long, String, String>>> t)
							throws Exception {
						List<Tuple6<String, Long, Float, Long, String, String>> list = Lists.newArrayList(t._2);
						Collections.sort(list, MyTupleComparator.INSTANCE);

						list = list.stream().limit(topN).collect(Collectors.toList());

						Float amount = 0.0f;

						Long topMerchantsOrder = 0l;

						for (Tuple6<String, Long, Float, Long, String, String> tuple : list) {
							amount += tuple._3();
							topMerchantsOrder += tuple._4();
						}

						Tuple2<Float, Long> amountOrderTuple = bankTotalAmountMap.get(t._1);

						return Arrays.asList(new Tuple5<String, String, Long, String, Long>(t._1,
								formatter.format(amountOrderTuple._1()), amountOrderTuple._2,
								formatter.format(((amount * 100.0f) / amountOrderTuple._1())) + " %",
								topMerchantsOrder)).iterator();
					}
				});

		// Bank Total Transaction Value (in 2016) Total Transaction Count (in 2016)
		// Transaction Contribution of top 10 Merchants (2016) Total Transaction Count
		// by top 10 Merchants(2016)
		System.out.println("Output = " + finalAnswer.collect());
	}

	private static void topMerchantTxAmountQuarterly(List<Long> merchantNameList,
			JavaRDD<Tuple6<String, Long, Float, Long, String, String>> bankMerchantPairRDD) {
		JavaRDD<Tuple2<Long, List<Tuple2<String, Float>>>> merchantTxAmountQuarterly = bankMerchantPairRDD
				.filter(new Function<Tuple6<String, Long, Float, Long, String, String>, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple6<String, Long, Float, Long, String, String> v1) throws Exception {
						return merchantNameList.indexOf(v1._2()) != -1;
					}
				}).map(new Function<Tuple6<String, Long, Float, Long, String, String>, Tuple3<Long, String, Float>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple3<Long, String, Float> call(Tuple6<String, Long, Float, Long, String, String> v1)
							throws Exception {

						return new Tuple3<Long, String, Float>(v1._2(), " Quarter " + (v1._5()), v1._3());
					}
				}).groupBy(new Function<Tuple3<Long, String, Float>, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Tuple3<Long, String, Float> v1) throws Exception {
						return v1._1();
					}
				})
				.map(new Function<Tuple2<Long, Iterable<Tuple3<Long, String, Float>>>, Tuple2<Long, List<Tuple2<String, Float>>>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, List<Tuple2<String, Float>>> call(
							Tuple2<Long, Iterable<Tuple3<Long, String, Float>>> v1) throws Exception {
						List<Tuple3<Long, String, Float>> tuples = Lists.newArrayList(v1._2);

						List<Tuple2<String, Float>> quarterGroupBy = javaSparkContext.parallelize(tuples)
								.mapToPair(t -> new Tuple2<String, Float>(t._2(), t._3()))
								.reduceByKey(new Function2<Float, Float, Float>() {

									private static final long serialVersionUID = 1L;

									@Override
									public Float call(Float v1, Float v2) throws Exception {
										return v1 + v2;
									}
								}).collect();

						return new Tuple2<Long, List<Tuple2<String, Float>>>(v1._1, quarterGroupBy);
					}
				});

		// Top merchant transaction amount quarterly.
		System.out.println("Top merchant amount quarterly = " + merchantTxAmountQuarterly.collect());
	}

	static class MyTupleComparator
			implements Comparator<Tuple6<String, Long, Float, Long, String, String>>, Serializable {

		private static final long serialVersionUID = 1L;
		final static MyTupleComparator INSTANCE = new MyTupleComparator();

		@Override
		public int compare(Tuple6<String, Long, Float, Long, String, String> o1,
				Tuple6<String, Long, Float, Long, String, String> o2) {
			return -o1._3().compareTo(o2._3());
		}

	}

	static class Comparator2 implements Comparator<Entry<String, Tuple2<Float, Long>>>, Serializable {

		private static final long serialVersionUID = 1L;
		final static Comparator2 INSTANCE = new Comparator2();

		@Override
		public int compare(Entry<String, Tuple2<Float, Long>> o1, Entry<String, Tuple2<Float, Long>> o2) {

			return -o1.getValue()._1.compareTo(o2.getValue()._1);
		}

	}

	static class Comparator3 implements Comparator<Entry<Long, Float>>, Serializable {

		private static final long serialVersionUID = 1L;
		final static Comparator3 INSTANCE = new Comparator3();

		@Override
		public int compare(Entry<Long, Float> o1, Entry<Long, Float> o2) {
			return -o1.getValue().compareTo(o2.getValue());
		}

	}

	static class Comparator4 implements Comparator<Entry<String, Float>>, Serializable {

		private static final long serialVersionUID = 1L;
		final static Comparator4 INSTANCE = new Comparator4();

		@Override
		public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
			return -o1.getValue().compareTo(o2.getValue());
		}

	}
}