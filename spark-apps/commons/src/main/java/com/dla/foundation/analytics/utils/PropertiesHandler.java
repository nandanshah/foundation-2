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
	private String PROPERTIES_FILE_VAR = "fisproperties";
	private String fisPropFilePath = System.getProperty(PROPERTIES_FILE_VAR);

	public PropertiesHandler() throws IOException {
		try {
			FileInputStream input = new FileInputStream(fisPropFilePath);
			props.load(input);
		} catch (IOException e) {
			throw e;
		}
	}

	public PropertiesHandler(String pFileName) throws IOException {
		fisPropFilePath = pFileName;
		try {
			FileInputStream input = new FileInputStream(fisPropFilePath);
			props.load(input);
		} catch (IOException e) {
			throw e;
		}
	}

	public String getValue(String key) throws IOException {
		String value = props.getProperty(key);
		if (value == null)
			throw new IOException("Property '" + key
					+ "' is not present in property file : " + fisPropFilePath);
		return value.trim();
	}

	public String getValue(String key, String defaultValue) {
		String value = props.getProperty(key);
		if (null != value && 0 != "".compareTo(value))
			return value.trim();
		return defaultValue;
	}

	/**
	 * Get property value of property 'propKey' from FIS' 
	 * common properties file.
	 * 
	 * @param propKey The property key for which value is to be read 
	 * @return Property value with key 'propKey'
	 * @throws IOException If property with key propKey is not found. 
	 */
	public String getValue(CommonPropKeys propKey) throws IOException {
		String value = props.getProperty(propKey.getValue());
		if (value == null)
			throw new IOException("Property '" + propKey.getValue()
					+ "' is not present in property file : " + fisPropFilePath);
		return value.trim();
	}

	/**
	 * Get property value of property 'propKey' from FIS' 
	 * common properties file. If property is not found in the
	 * file, defaultValue is returned.
	 * 
	 * @param propKey The property key for which value is to be read 
	 * @param defaultValue The default property value if not present in file
	 * @return Property value with key 'propKey', defaultValue if not found
	 */
	public String getValue(CommonPropKeys cpe, String defaultValue) {
		String value = props.getProperty(cpe.getValue());
		if (null != value && 0 != "".compareTo(value))
			return value.trim();
		return defaultValue;
	}
}
