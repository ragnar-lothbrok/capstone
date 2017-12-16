package com.edureka.capstone.dtos;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "merchantId", "merchantName", "mobileNumber", "startDate", "email", "address", "state ",
		"country ", "pincode ", "merchantType ", "segment ", "taxRegNum ", "description " })
public class MerchantDto {

	@JsonProperty("merchantId")
	private Integer merchantId;
	@JsonProperty("merchantName")
	private String merchantName;
	@JsonProperty("mobileNumber")
	private Integer mobileNumber;
	@JsonProperty("startDate")
	private Integer startDate;
	@JsonProperty("email")
	private String email;
	@JsonProperty("address")
	private String address;
	@JsonProperty("state ")
	private String state;
	@JsonProperty("country ")
	private String country;
	@JsonProperty("pincode ")
	private Integer pincode;
	@JsonProperty("merchantType ")
	private Integer merchantType;
	@JsonProperty("segment ")
	private String segment;
	@JsonProperty("taxRegNum ")
	private String taxRegNum;
	@JsonProperty("description ")
	private String description;
	@JsonIgnore
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();

	@JsonProperty("merchantId")
	public Integer getMerchantId() {
		return merchantId;
	}

	@JsonProperty("merchantId")
	public void setMerchantId(Integer merchantId) {
		this.merchantId = merchantId;
	}

	@JsonProperty("merchantName")
	public String getMerchantName() {
		return merchantName;
	}

	@JsonProperty("merchantName")
	public void setMerchantName(String merchantName) {
		this.merchantName = merchantName;
	}

	@JsonProperty("mobileNumber")
	public Integer getMobileNumber() {
		return mobileNumber;
	}

	@JsonProperty("mobileNumber")
	public void setMobileNumber(Integer mobileNumber) {
		this.mobileNumber = mobileNumber;
	}

	@JsonProperty("startDate")
	public Integer getStartDate() {
		return startDate;
	}

	@JsonProperty("startDate")
	public void setStartDate(Integer startDate) {
		this.startDate = startDate;
	}

	@JsonProperty("email")
	public String getEmail() {
		return email;
	}

	@JsonProperty("email")
	public void setEmail(String email) {
		this.email = email;
	}

	@JsonProperty("address")
	public String getAddress() {
		return address;
	}

	@JsonProperty("address")
	public void setAddress(String address) {
		this.address = address;
	}

	@JsonProperty("state ")
	public String getState() {
		return state;
	}

	@JsonProperty("state ")
	public void setState(String state) {
		this.state = state;
	}

	@JsonProperty("country ")
	public String getCountry() {
		return country;
	}

	@JsonProperty("country ")
	public void setCountry(String country) {
		this.country = country;
	}

	@JsonProperty("pincode ")
	public Integer getPincode() {
		return pincode;
	}

	@JsonProperty("pincode ")
	public void setPincode(Integer pincode) {
		this.pincode = pincode;
	}

	@JsonProperty("merchantType ")
	public Integer getMerchantType() {
		return merchantType;
	}

	@JsonProperty("merchantType ")
	public void setMerchantType(Integer merchantType) {
		this.merchantType = merchantType;
	}

	@JsonProperty("segment ")
	public String getSegment() {
		return segment;
	}

	@JsonProperty("segment ")
	public void setSegment(String segment) {
		this.segment = segment;
	}

	@JsonProperty("taxRegNum ")
	public String getTaxRegNum() {
		return taxRegNum;
	}

	@JsonProperty("taxRegNum ")
	public void setTaxRegNum(String taxRegNum) {
		this.taxRegNum = taxRegNum;
	}

	@JsonProperty("description ")
	public String getDescription() {
		return description;
	}

	@JsonProperty("description ")
	public void setDescription(String description) {
		this.description = description;
	}

	@JsonAnyGetter
	public Map<String, Object> getAdditionalProperties() {
		return this.additionalProperties;
	}

	@JsonAnySetter
	public void setAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
	}

	@Override
	public String toString() {
		return "MerchantDto [merchantId=" + merchantId + ", merchantName=" + merchantName + ", mobileNumber="
				+ mobileNumber + ", startDate=" + startDate + ", email=" + email + ", address=" + address + ", state="
				+ state + ", country=" + country + ", pincode=" + pincode + ", merchantType=" + merchantType
				+ ", segment=" + segment + ", taxRegNum=" + taxRegNum + ", description=" + description
				+ ", additionalProperties=" + additionalProperties + "]";
	}

}
