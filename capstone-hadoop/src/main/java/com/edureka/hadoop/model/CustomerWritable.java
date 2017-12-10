package com.edureka.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.gson.Gson;

public class CustomerWritable implements WritableComparable<CustomerWritable> {

	private static Gson gson = new Gson();

	private CustomerKey customerKey = new CustomerKey();

	public CustomerWritable() {

	}

	public CustomerWritable(CustomerKey customerKey) {
		super();
		this.customerKey = customerKey;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeLong(customerKey.getCustomerKey());
		out.writeUTF(customerKey.getDate());
	}

	public CustomerKey getCustomerKey() {
		return customerKey;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		customerKey.setCustomerKey(in.readLong());
		customerKey.setDate(in.readUTF());
	}

	@Override
	public String toString() {
		return gson.toJson(customerKey);
	}

	@Override
	public int compareTo(CustomerWritable o) {
		return this.getCustomerKey().compareTo(o.getCustomerKey());
	}
}
