package com.edureka.hadoop.model;

import java.io.Serializable;

public class CustomerKey implements Serializable, Comparable<CustomerKey> {

	private static final long serialVersionUID = 1L;

	private Long customerKey;
	private String date;

	public Long getCustomerKey() {
		return customerKey;
	}

	public void setCustomerKey(Long customerKey) {
		this.customerKey = customerKey;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	@Override
	public int compareTo(CustomerKey o) {
		return this.getCustomerKey().compareTo(o.getCustomerKey()) == 0 ? this.getDate().compareTo(o.getDate())
				: this.getCustomerKey().compareTo(o.getCustomerKey());
	}

}
