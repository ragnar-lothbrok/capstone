package com.edureka.capstone;

import java.io.Serializable;

public class MerchantTransactionCounter implements Serializable {

	private static final long serialVersionUID = 1L;

	private Long merchantid;
	private Long ordersuccesscounter = 0l;
	private Long ordercancelcounter = 0l;
	private Long orderbelow500 = 0l;
	private Long orderbelow1000 = 0l;
	private Long orderbelow2000 = 0l;
	private Long orderabove2000 = 0l;

	public Long getMerchantid() {
		return merchantid;
	}

	public void setMerchantid(Long merchantid) {
		this.merchantid = merchantid;
	}

	public Long getOrdersuccesscounter() {
		return ordersuccesscounter;
	}

	public void setOrdersuccesscounter(Long ordersuccesscounter) {
		this.ordersuccesscounter = ordersuccesscounter;
	}

	public Long getOrdercancelcounter() {
		return ordercancelcounter;
	}

	public void setOrdercancelcounter(Long ordercancelcounter) {
		this.ordercancelcounter = ordercancelcounter;
	}

	public Long getOrderbelow500() {
		return orderbelow500;
	}

	public void setOrderbelow500(Long orderbelow500) {
		this.orderbelow500 = orderbelow500;
	}

	public Long getOrderbelow1000() {
		return orderbelow1000;
	}

	public void setOrderbelow1000(Long orderbelow1000) {
		this.orderbelow1000 = orderbelow1000;
	}

	public Long getOrderbelow2000() {
		return orderbelow2000;
	}

	public void setOrderbelow2000(Long orderbelow2000) {
		this.orderbelow2000 = orderbelow2000;
	}

	public Long getOrderabove2000() {
		return orderabove2000;
	}

	public void setOrderabove2000(Long orderabove2000) {
		this.orderabove2000 = orderabove2000;
	}

	public MerchantTransactionCounter(Long merchantid, Long ordersuccesscounter, Long ordercancelcounter,
			Long orderbelow500, Long orderbelow1000, Long orderbelow2000, Long orderabove2000) {
		super();
		this.merchantid = merchantid;
		this.ordersuccesscounter = ordersuccesscounter;
		this.ordercancelcounter = ordercancelcounter;
		this.orderbelow500 = orderbelow500;
		this.orderbelow1000 = orderbelow1000;
		this.orderbelow2000 = orderbelow2000;
		this.orderabove2000 = orderabove2000;
	}

	public MerchantTransactionCounter() {

	}

}
