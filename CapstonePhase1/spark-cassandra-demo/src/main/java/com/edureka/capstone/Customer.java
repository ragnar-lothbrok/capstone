package com.edureka.capstone;

import java.io.Serializable;

public class Customer implements Serializable {

	private static final long serialVersionUID = 1L;

	private Long customerid;
	private String name;
	private String mobilenumber;
	private String gender;
	private long birthdate;
	private String email;
	private String address;
	private String state;
	private String country;
	private Long pincode;
	private String title;
	private String martial_status;
	private Long created;

	public Long getCustomerid() {
		return customerid;
	}

	public void setCustomerid(Long customerid) {
		this.customerid = customerid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMobilenumber() {
		return mobilenumber;
	}

	public void setMobilenumber(String mobilenumber) {
		this.mobilenumber = mobilenumber;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public long getBirthdate() {
		return birthdate;
	}

	public void setBirthdate(long birthdate) {
		this.birthdate = birthdate;
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

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getMartial_status() {
		return martial_status;
	}

	public void setMartial_status(String martial_status) {
		this.martial_status = martial_status;
	}

	public Long getCreated() {
		return created;
	}

	public void setCreated(Long created) {
		this.created = created;
	}

	public Customer(Long id, String name, String mobilenumber, String gender, long birthdate, String email,
			String address, String state, String country, Long pincode, String title, String martial_status,
			Long created) {
		super();
		this.customerid = id;
		this.name = name;
		this.mobilenumber = mobilenumber;
		this.gender = gender;
		this.birthdate = birthdate;
		this.email = email;
		this.address = address;
		this.state = state;
		this.country = country;
		this.pincode = pincode;
		this.title = title;
		this.martial_status = martial_status;
		this.created = created;
	}

}
