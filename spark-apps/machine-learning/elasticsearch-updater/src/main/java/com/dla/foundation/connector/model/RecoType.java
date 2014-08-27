package com.dla.foundation.connector.model;

public class RecoType {
	String active;
	String passive;
	
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
