package com.edureka.capstone;

import java.io.Serializable;

public class BankMerchantTransaction implements Serializable {

	private static final long serialVersionUID = 1L;

	private Integer year;
	private Integer month;
	private String bank;
	private Long merchantid;
	private float totalamount;
	private Long ordercount;
	private String segment;

	public String getSegment() {
		return segment;
	}

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

	public void setSegment(String segment) {
		this.segment = segment;
	}

	public String getBank() {
		return bank;
	}

	public void setBank(String bank) {
		this.bank = bank;
	}

	public Long getMerchantid() {
		return merchantid;
	}

	public void setMerchantid(Long merchantid) {
		this.merchantid = merchantid;
	}

	public float getTotalamount() {
		return totalamount;
	}

	public void setTotalamount(float totalamount) {
		this.totalamount = totalamount;
	}

	public Long getOrdercount() {
		return ordercount;
	}

	public void setOrdercount(Long ordercount) {
		this.ordercount = ordercount;
	}

	public BankMerchantTransaction(String bank, Long merchantid, float totalamount, Long orderCount,
			String segment, Integer year, Integer month) {
		super();
		this.bank = bank;
		this.merchantid = merchantid;
		this.totalamount = totalamount;
		this.ordercount = orderCount;
		this.segment = segment;
		this.year = year;
		this.month = month;
	}

}
