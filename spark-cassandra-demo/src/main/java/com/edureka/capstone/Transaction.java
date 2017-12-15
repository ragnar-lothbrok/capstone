package com.edureka.capstone;

import java.io.Serializable;

public class Transaction implements Serializable {

	private static final long serialVersionUID = 1L;
	private String transactionid;
	private Long customerid;
	private Long merchantid;
	private String status;
	private long timestamp;
	private String invoicenumber;
	private float invoiceamount;
	private String segment;

	public String getSegment() {
		return segment;
	}

	public void setSegment(String segment) {
		this.segment = segment;
	}

	public String getTransactionid() {
		return transactionid;
	}

	public void setTransactionid(String transactionid) {
		this.transactionid = transactionid;
	}

	public Long getCustomerid() {
		return customerid;
	}

	public void setCustomerid(Long customerid) {
		this.customerid = customerid;
	}

	public Long getMerchantid() {
		return merchantid;
	}

	public void setMerchantid(Long merchantid) {
		this.merchantid = merchantid;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getInvoicenumber() {
		return invoicenumber;
	}

	public void setInvoicenumber(String invoicenumber) {
		this.invoicenumber = invoicenumber;
	}

	public float getInvoiceamount() {
		return invoiceamount;
	}

	public void setInvoiceamount(float invoiceamount) {
		this.invoiceamount = invoiceamount;
	}

	public Transaction(String transactionid, Long customerid, Long merchantid, String status, long timestamp,
			String invoicenumber, float invoiceamount, String segment) {
		super();
		this.transactionid = transactionid;
		this.customerid = customerid;
		this.merchantid = merchantid;
		this.status = status;
		this.timestamp = timestamp;
		this.invoicenumber = invoicenumber;
		this.invoiceamount = invoiceamount;
		this.segment = segment;
	}

}
