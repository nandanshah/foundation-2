package com.dla.foundation.fis.eo.entities;

public enum DeviceType {

	IPHONE("iphone"), IPAD("ipad"), IOS_OTHER("ios_other"), ANDROID_PHONE(
			"android_phone"), ANDROID_TAB("android_tab"), ANDROID_OTHER(
			"android_other"), APPLE_TV("apple_tv"), CHROMECAST("chromecast"), LAPTOP(
			"laptop"), DESKTOP("desktop"), SMART_TV("smart_tv"), OTHER("other");
	private String deviceType;

	private DeviceType(String deviceType) {
		this.deviceType = deviceType;
	}

	public String getDeviceType() {
		return deviceType;
	}

}
