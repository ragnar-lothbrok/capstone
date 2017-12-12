package com.edureka.capstone;

import java.io.Serializable;

public class BankMerchantTransaction implements Serializable {

	private static final long serialVersionUID = 1L;

	private String date;
	private String bank;
	private Long merchantid;
	private float totalamount;
	private Long ordercount;

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
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

	public BankMerchantTransaction(String date, String bank, Long merchantid, float totalamount, Long orderCount) {
		super();
		this.date = date;
		this.bank = bank;
		this.merchantid = merchantid;
		this.totalamount = totalamount;
		this.ordercount = orderCount;
	}

}
