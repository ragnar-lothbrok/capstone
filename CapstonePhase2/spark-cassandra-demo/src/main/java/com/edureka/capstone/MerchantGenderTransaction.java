package com.edureka.capstone;

import java.io.Serializable;

public class MerchantGenderTransaction implements Serializable {

	private static final long serialVersionUID = 1L;

	private Integer year;
	private Integer month;
	private Long merchantid;
	private float amount;
	private String gender;

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}

	public Long getMerchantid() {
		return merchantid;
	}

	public void setMerchantid(Long merchantid) {
		this.merchantid = merchantid;
	}

	public float getAmount() {
		return amount;
	}

	public void setAmount(float amount) {
		this.amount = amount;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public MerchantGenderTransaction(Integer year, Integer month, Long merchantid, float amount, String gender) {
		super();
		this.year = year;
		this.month = month;
		this.merchantid = merchantid;
		this.amount = amount;
		this.gender = gender;
	}

}
