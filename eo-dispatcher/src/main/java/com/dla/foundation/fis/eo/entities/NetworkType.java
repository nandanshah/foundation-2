package com.dla.foundation.fis.eo.entities;

public enum NetworkType {

	LAN("lan"),
	WIFI("wifi"),
	N_3G("3g"),
	N_4G("4g"),
	N_4GLTE("4gLTE"),
	N_2G("2g"),
	OTHER("other");

	private String networkType;

	private NetworkType(String networkType) {
		this.networkType = networkType;
	}

	public String getNetworkType() {
		return networkType;
	}
}
