package com.edureka.capstone;

import java.io.Serializable;

public class CardDetails implements Serializable {

	private static final long serialVersionUID = 1L;

	private Long cardid;
	private Long customerid;
	private String bank;
	private String type;
	private Integer expirymonth;
	private Integer expiryyear;
	private String cardnum;
	private Float cardlimit;
	private Integer valid_month;
	private Integer valid_year;

	public CardDetails(Long cardId, Long customerId, String bank, String type, Integer expiryMonth, Integer expiryYear,
			String cardNum, Float cardLimit, Integer valid_month, Integer valid_year) {
		super();
		this.cardid = cardId;
		this.customerid = customerId;
		this.bank = bank;
		this.type = type;
		this.expirymonth = expiryMonth;
		this.expiryyear = expiryYear;
		this.cardnum = cardNum;
		this.cardlimit = cardLimit;
		this.valid_month = valid_month;
		this.valid_year = valid_year;
	}

	public Integer getValid_month() {
		return valid_month;
	}

	public void setValid_month(Integer valid_month) {
		this.valid_month = valid_month;
	}

	public Integer getValid_year() {
		return valid_year;
	}

	public void setValid_year(Integer valid_year) {
		this.valid_year = valid_year;
	}

	public Long getCardid() {
		return cardid;
	}

	public void setCardid(Long cardid) {
		this.cardid = cardid;
	}

	public Long getCustomerid() {
		return customerid;
	}

	public void setCustomerid(Long customerid) {
		this.customerid = customerid;
	}

	public String getBank() {
		return bank;
	}

	public void setBank(String bank) {
		this.bank = bank;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Integer getExpirymonth() {
		return expirymonth;
	}

	public void setExpirymonth(Integer expirymonth) {
		this.expirymonth = expirymonth;
	}

	public Integer getExpiryyear() {
		return expiryyear;
	}

	public void setExpiryyear(Integer expiryyear) {
		this.expiryyear = expiryyear;
	}

	public String getCardnum() {
		return cardnum;
	}

	public void setCardnum(String cardnum) {
		this.cardnum = cardnum;
	}

	public Float getCardlimit() {
		return cardlimit;
	}

	public void setCardlimit(Float cardlimit) {
		this.cardlimit = cardlimit;
	}

	public CardDetails() {

	}

}
