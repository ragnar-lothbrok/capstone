package com.edureka.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.google.gson.Gson;

public class AggregateWritable implements Writable {

	private static Gson gson = new Gson();

	private AggregateData aggregateData = new AggregateData();

	public AggregateWritable() {

	}

	public AggregateWritable(AggregateData aggregateData) {
		super();
		this.aggregateData = aggregateData;
	}

	public AggregateData getAggregateData() {
		return aggregateData;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeLong(aggregateData.getOrdersuccesscounter());
		out.writeLong(aggregateData.getOrdercancelcounter());
		out.writeLong(aggregateData.getOrderbelow500());
		out.writeLong(aggregateData.getOrderbelow1000());
		out.writeLong(aggregateData.getOrderbelow2000());
		out.writeLong(aggregateData.getOrderabove2000());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		aggregateData.setOrdersuccesscounter(in.readLong());
		aggregateData.setOrdercancelcounter(in.readLong());
		aggregateData.setOrderbelow500(in.readLong());
		aggregateData.setOrderbelow1000(in.readLong());
		aggregateData.setOrderbelow2000(in.readLong());
		aggregateData.setOrderabove2000(in.readLong());
	}

	@Override
	public String toString() {
		return gson.toJson(aggregateData);
	}
}
