package com.edureka.hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.edureka.hadoop.model.AggregateData;
import com.edureka.hadoop.model.AggregateWritable;
import com.edureka.hadoop.model.MerchantKey;
import com.edureka.hadoop.model.MerchantWritable;
import com.edureka.hadoop.model.Transaction;
import com.google.gson.Gson;

public class DailyMerchantTransactionCountMRJob {

	public static class TransactionMapper extends Mapper<Object, Text, MerchantWritable, AggregateWritable> {

		static SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy");

		// Map method
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			Transaction transaction = new Gson().fromJson(line, Transaction.class);

			AggregateData aggregateData = new AggregateData();

			AggregateWritable aggregateWritable = new AggregateWritable(aggregateData);

			if (transaction.getStatus().equalsIgnoreCase("SUCCESS")) {
				aggregateData.setOrdersuccesscounter(1l);
			} else {
				aggregateData.setOrdercancelcounter(1l);
			}

			if (transaction.getInvoiceAmount() <= 500) {
				aggregateData.setOrderbelow500(1l);
			} else if (transaction.getInvoiceAmount() > 500 && transaction.getInvoiceAmount() <= 1000) {
				aggregateData.setOrderbelow1000(1l);
			} else if (transaction.getInvoiceAmount() > 1000 && transaction.getInvoiceAmount() < 2000) {
				aggregateData.setOrderbelow2000(1l);
			} else {
				aggregateData.setOrderabove2000(1l);
			}

			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(transaction.getTimestamp());

			MerchantKey merchantKey = new MerchantKey();
			merchantKey.setDate(sdf.format(cal.getTime()));
			merchantKey.setMerchantKey(transaction.getMerchantId());

			context.write(new MerchantWritable(merchantKey), aggregateWritable);
		}

	}

	public static class TxAggregator
			extends Reducer<MerchantWritable, AggregateWritable, MerchantWritable, AggregateWritable> {

		public void reduce(MerchantWritable key, Iterable<AggregateWritable> values, Context context)
				throws IOException, InterruptedException {

			AggregateData aggregateData = new AggregateData();
			AggregateWritable aggregateWritable = new AggregateWritable(aggregateData);

			for (AggregateWritable val : values) {
				aggregateData.setOrderabove2000(
						aggregateData.getOrderabove2000() + val.getAggregateData().getOrderabove2000());
				aggregateData.setOrderbelow1000(
						aggregateData.getOrderbelow1000() + val.getAggregateData().getOrderbelow1000());
				aggregateData.setOrderbelow2000(
						aggregateData.getOrderbelow2000() + val.getAggregateData().getOrderbelow2000());
				aggregateData
						.setOrderbelow500(aggregateData.getOrderbelow500() + val.getAggregateData().getOrderbelow500());
				aggregateData.setOrdersuccesscounter(
						aggregateData.getOrdersuccesscounter() + val.getAggregateData().getOrdersuccesscounter());
				aggregateData.setOrdercancelcounter(
						aggregateData.getOrdercancelcounter() + val.getAggregateData().getOrdercancelcounter());

			}
			context.write(key, aggregateWritable);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Daily merchant Tx Count Job");
		job.setJarByClass(DailyMerchantTransactionCountMRJob.class);
		job.setMapperClass(TransactionMapper.class);
		job.setCombinerClass(TxAggregator.class);
		job.setReducerClass(TxAggregator.class);
		job.setOutputKeyClass(MerchantWritable.class);
		job.setOutputValueClass(AggregateWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("/Users/output/" + Calendar.getInstance().getTimeInMillis()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
