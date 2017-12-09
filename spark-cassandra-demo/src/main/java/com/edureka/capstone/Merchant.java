package com.edureka.capstone;

import java.io.Serializable;

public class Merchant implements Serializable {

	private static final long serialVersionUID = 1L;

	private Long merchantid;
	private String merchantname;
	private String email;
	private String address;
	private String state;
	private String country;
	private Long pincode;
	private String segment;
	private String taxregnum;
	private String description;
	private Long startdate;
	private Integer merchanttype;
	private String mobilenumber;

	public Merchant(Long merchantid, String merchantname, String email, String address, String state, String country,
			Long pincode, String category, String taxReg, String description, Long startdate, Integer merchanttype,
			String mobileNumber) {
		super();
		this.merchantid = merchantid;
		this.merchantname = merchantname;
		this.email = email;
		this.address = address;
		this.state = state;
		this.country = country;
		this.pincode = pincode;
		this.segment = category;
		this.taxregnum = taxReg;
		this.description = description;
		this.startdate = startdate;
		this.merchanttype = merchanttype;
		this.mobilenumber = mobileNumber;
	}

	public Long getMerchantid() {
		return merchantid;
	}

	public void setMerchantid(Long merchantid) {
		this.merchantid = merchantid;
	}

	public String getMerchantname() {
		return merchantname;
	}

	public void setMerchantname(String merchantname) {
		this.merchantname = merchantname;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public Long getPincode() {
		return pincode;
	}

	public void setPincode(Long pincode) {
		this.pincode = pincode;
	}

	public String getSegment() {
		return segment;
	}

	public void setSegment(String segment) {
		this.segment = segment;
	}

	public String getTaxregnum() {
		return taxregnum;
	}

	public void setTaxregnum(String taxregnum) {
		this.taxregnum = taxregnum;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Long getStartdate() {
		return startdate;
	}

	public void setStartdate(Long startdate) {
		this.startdate = startdate;
	}

	public Integer getMerchanttype() {
		return merchanttype;
	}

	public void setMerchanttype(Integer merchanttype) {
		this.merchanttype = merchanttype;
	}

	public String getMobilenumber() {
		return mobilenumber;
	}

	public void setMobilenumber(String mobilenumber) {
		this.mobilenumber = mobilenumber;
	}

}
