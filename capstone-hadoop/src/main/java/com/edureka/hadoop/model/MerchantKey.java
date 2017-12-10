package com.edureka.hadoop.model;

import java.io.Serializable;

public class MerchantKey implements Serializable, Comparable<MerchantKey> {

	private static final long serialVersionUID = 1L;

	private Long merchantKey;
	private String date;

	public Long getMerchantKey() {
		return merchantKey;
	}

	public void setMerchantKey(Long merchantKey) {
		this.merchantKey = merchantKey;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	@Override
	public int compareTo(MerchantKey o) {
		return this.getMerchantKey().compareTo(o.getMerchantKey()) == 0 ? this.getDate().compareTo(o.getDate())
				: this.getMerchantKey().compareTo(o.getMerchantKey());
	}

}
