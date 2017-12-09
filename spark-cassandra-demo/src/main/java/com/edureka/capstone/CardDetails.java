package com.edureka.capstone;

import java.io.Serializable;

public class CardDetails implements Serializable {

	private static final long serialVersionUID = 1L;

	private Long cardid;
	private Long customerid;
	private String bank;
	private String type;
	private String nameoncard;
	private Integer expirymonth;
	private Integer expiryyear;
	private String cardnum;
	private Float cardlimit;

	public CardDetails(Long cardId, Long customerId, String bank, String type, String nameOnCard, Integer expiryMonth,
			Integer expiryYear, String cardNum, Float cardLimit) {
		super();
		this.cardid = cardId;
		this.customerid = customerId;
		this.bank = bank;
		this.type = type;
		this.nameoncard = nameOnCard;
		this.expirymonth = expiryMonth;
		this.expiryyear = expiryYear;
		this.cardnum = cardNum;
		this.cardlimit = cardLimit;
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

	public String getNameoncard() {
		return nameoncard;
	}

	public void setNameoncard(String nameoncard) {
		this.nameoncard = nameoncard;
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
