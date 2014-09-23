package com.dla.foundation.connector.model;

public class RecoType {
	String active;
	String passive;
	public final String defaultProfileId  = "9769e61f-238f-11b2-7f7f-7f7f7f7f7f7f";
	
	public RecoType(){}
	public RecoType(String active, String passive){
		System.out.println("Recotype contructor called");
		this.active=active;
		this.passive=passive;
	}
	
	public String getActive() {
		return active;
	}
	public void setActive(String active) {
		this.active = active;
	}
	public String getPassive() {
		return passive;
	}
	public void setPassive(String passive) {
		this.passive = passive;
	}
	
	
}
