package com.dla.foundation.analytics.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created this simple properties handler class so that properties handling part
 * can be pluggable and we can plug other features like reading properties from
 * say servletcontext or any other source in future.
 * 
 */
public class PropertiesHandler {

	public static final Properties props = new Properties();
	private final String fileName;

	public PropertiesHandler(String pFileName) throws IOException {
		fileName = pFileName;
		try {
			FileInputStream input = new FileInputStream(fileName);
			props.load(input);
		} catch (IOException e) {
			throw e;
		}
	}

	public String getValue(String key) throws IOException {
		String value = props.getProperty(key);
		if (value == null)
			throw new IOException("Property '" + key
					+ "' is not present in property file : " + fileName);
		return value.trim();
	}

	public String getValue(String key, String defaultValue) {
		String value = props.getProperty(key);
		if (null != value && 0 != "".compareTo(value))
			return value.trim();
		return defaultValue;
	}

}
