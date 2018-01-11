package com.edureka.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.gson.Gson;

public class MerchantWritable implements WritableComparable<MerchantWritable> {

	private static Gson gson = new Gson();

	private MerchantKey merchantKey = new MerchantKey();

	public MerchantWritable() {

	}

	public MerchantKey getMerchantKey() {
		return merchantKey;
	}

	public void setMerchantKey(MerchantKey merchantKey) {
		this.merchantKey = merchantKey;
	}

	public MerchantWritable(MerchantKey MerchantKey) {
		super();
		this.merchantKey = MerchantKey;
	}

	public void write(DataOutput out) throws IOException {

		out.writeLong(merchantKey.getMerchantKey());
		out.writeUTF(merchantKey.getDate());
	}

	public void readFields(DataInput in) throws IOException {
		merchantKey.setMerchantKey(in.readLong());
		merchantKey.setDate(in.readUTF());
	}

	@Override
	public String toString() {
		return gson.toJson(merchantKey);
	}

	@Override
	public int compareTo(MerchantWritable o) {
		return this.getMerchantKey().compareTo(o.getMerchantKey());
	}
}
