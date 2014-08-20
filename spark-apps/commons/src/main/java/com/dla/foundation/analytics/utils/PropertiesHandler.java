package com.dla.foundation.analytics.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import com.datastax.driver.core.ResultSet;

/**
 * Created this simple properties handler class so that properties handling part
 * can be pluggable and we can plug other features like reading properties from
 * say servletcontext or any other source in future.
 * 
 */
public class PropertiesHandler {

	public static final Properties propsInFile = new Properties();
	private static final String propertiesCol = "properties";
	private String PROPERTIES_FILE_VAR = "cupropertiesfile";
	private String propertiesFilePath = System.getProperty(PROPERTIES_FILE_VAR);

	private final String fileName;
	private CassandraContext csContext;
	private Map<String, String> propsInCS;
	private String appName, keyspace, colFamily, appnameCol;
	String[] csHostList;

	/**
	 * Initializes a new PropertiesHandler which loads common properties. It
	 * takes jvm arguments(using -D) to locate common properties file.
	 * 
	 * @throws IOException
	 */
	public PropertiesHandler() throws IOException {
		fileName = propertiesFilePath;
		try {
			FileInputStream input = new FileInputStream(fileName);
			propsInFile.load(input);
		} catch (IOException e) {
			throw e;
		}
	}

	/**
	 * Initializes a new PropertiesHandler which loads properties files using
	 * specified properties file path 'pFileName'
	 * 
	 * @param pFileName
	 *            properties file path
	 * @throws IOException
	 *             throws exception if file path not found
	 */
	public PropertiesHandler(String pFileName) throws IOException {
		fileName = pFileName;
		try {
			FileInputStream input = new FileInputStream(fileName);
			propsInFile.load(input);
		} catch (IOException e) {
			throw e;
		}
	}

	/**
	 * Initializes a new PropertiesHandler which loads properties files using
	 * specified properties file path 'pFileName' and reads properties in
	 * cassandra for specified spark appName
	 * 
	 * @param pFileName
	 *            properties file path
	 * @param appName
	 *            app name to read properties from cassandra
	 * @throws IOException
	 */
	public PropertiesHandler(String pFileName, String appName)
			throws IOException {
		this.fileName = pFileName;
		this.appName = appName;
		try {
			FileInputStream input = new FileInputStream(fileName);
			propsInFile.load(input);
			csContext = new CassandraContext(null);
			connectToCassandra();
			readFromCassandra();
		} catch (IOException e) {
			throw e;
		}
	}

	/**
	 * This method return property value for specified key if found in cassandra
	 * else from common properties. Returns null if not found in both.
	 * 
	 * @param key
	 *            property key
	 * @return property value for specified key or null
	 * @throws IOException
	 */
	public String getValue(String key) throws IOException {
		return readValue(key);
	}

	/**
	 * This method return property value for specified key if found in cassandra
	 * else from common properties. Returns default if not found in both.
	 * 
	 * @param key
	 *            property key
	 * @param defaultValue
	 *            return if value not exist
	 * @return property value for specified key or defualtValue
	 * @throws IOException
	 */
	public String getValue(String key, String defaultValue) throws IOException {
		String value = readValue(key);
		if (null != value)
			return value;
		return defaultValue;
	}



	/**
	 * This method returns property value for specified enum i.e. property key
	 * 
	 * @param cpe
	 *            property key
	 * @return property value
	 * @throws IOException
	 *             throws exception if value does not exist for specified key
	 */
	public String getValue(CommonPropKeys cpe) throws IOException {
		return readValue(cpe.toString());
	}

	/**
	 * This method returns property value for specified enum i.e. property key.
	 * If value does not exist, will return specified defaultValue.
	 * 
	 * @param cpe
	 *            property key
	 * @param defaultValue
	 * @return property value or defaultValue
	 * @throws IOException 
	 */
	public String getValue(CommonPropKeys cpe, String defaultValue) throws IOException {
		String value = readValue(cpe.toString());
		if (null != value)
			return value;
		return defaultValue;
	}

	/**
	 * This method will write key-value pair to given appname's properties map
	 * in cassandra. It will update value if key exist otherwise will insert new
	 * value.
	 * 
	 * @param key
	 *            - property key
	 * @param value
	 *            - property value for key
	 * @throws IOException
	 *             throws exception if PropertiesHandler does not initialized
	 *             with appName.
	 */
	public void writeToCassandra(String key, String value) throws IOException {
		if (null == appName)
			throw new IOException("appName should not be null");

		// Ex. UPDATE sparkApp SET properties['host'] = 'localhost' WHERE id =
		// 'pio';
		String cmd = "UPDATE " + keyspace + "." + colFamily + " SET "
				+ propertiesCol + "['" + key + "'] = '" + value + "' WHERE "
				+ appnameCol + "='" + appName + "'";

		csContext.executeCommand(cmd);
		readFromCassandra();
	}

	/**
	 * Read dynamic properties from Cassandra
	 */
	private void connectToCassandra() {
		try {
			keyspace = getValue(CommonPropKeys.cs_fisKeyspace);
			colFamily = getValue(CommonPropKeys.cs_sparkAppPropCF);
			appnameCol = getValue(CommonPropKeys.cs_sparkAppPropCol);
			csHostList = getValue(CommonPropKeys.cs_hostList).split(",");
		} catch (IOException e) {
			e.printStackTrace();
		}

		csContext.connect(csHostList);
	}

	/**
	 * Read dynamic properties from Cassandra
	 */
	private void readFromCassandra() {
		ResultSet rs = csContext.getRows(keyspace, colFamily, appnameCol,appName);
		propsInCS = rs.one().getMap(propertiesCol, String.class, String.class);
	}

	private String readValue(String key) throws IOException {
		if ((propsInCS != null) && propsInCS.containsKey(key))
			return propsInCS.get(key).trim();

		String value = propsInFile.getProperty(key);
		if (value == null)
			throw new IOException("Property '" + key
					+ "' is not present in property file : " + fileName
					+ " or in Cassandra");
		return value.trim();
	}
}