package com.dla.foundation.services.queue.util;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkFiles;
import org.json.JSONArray;
import org.json.JSONObject;

public class QueueListenerConfigHandler implements Serializable {

	private static final long serialVersionUID = -4419336790877201871L;

	private String rabbitmq_server_host;
	private int rabbitmq_server_port;
	private String exchange_name;
	private String exchange_type;
	private List<QueueConfig> qConfigs = new ArrayList<QueueConfig>();
	private String PROPERTIES_FILE_NAME = "QueueListenerConfig.json";
	private String PROPERTIES_FILE_VAR = "queueConfigFilePath";
	private String propertiesFilePath = System.getProperty(PROPERTIES_FILE_VAR);

	public static enum queue_type { async, sync };

	private enum QueueConfigKeys {
		rabbitmq_server_host("rabbitmq_server_host"), rabbitmq_server_port("rabbitmq_server_port"), 
		exchange_name("exchange_name"), exchange_type("exchange_type"),	queue_config("queue_config"), 
		queue_name("queue_name"), queue_type("queue_type"), queue_updater("queue_updater"), 
		queue_bind_key("queue_bind_key");

		private String value;

		QueueConfigKeys(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		@Override
		public String toString() {
			return value;
		}
	}

	public QueueListenerConfigHandler() throws IOException {
		if(propertiesFilePath == null)
			propertiesFilePath = SparkFiles.get(PROPERTIES_FILE_NAME);
		
		String content = new String(Files.readAllBytes(Paths.get(propertiesFilePath)));
		JSONObject configJson = new JSONObject(content);
		rabbitmq_server_host = configJson.getString(QueueConfigKeys.rabbitmq_server_host.getValue());
		rabbitmq_server_port = configJson.getInt(QueueConfigKeys.rabbitmq_server_port.getValue());
		exchange_name = configJson.getString(QueueConfigKeys.exchange_name.getValue());
		exchange_type = configJson.getString(QueueConfigKeys.exchange_type.getValue());

		JSONArray arr =  configJson.getJSONArray(QueueConfigKeys.queue_config.getValue());
		for (int i = 0; i < arr.length(); i++) {
			JSONObject oneQ = arr.getJSONObject(i);
			String server = oneQ.has(QueueConfigKeys.rabbitmq_server_host.getValue()) ? oneQ.getString(QueueConfigKeys.rabbitmq_server_host.getValue()) : rabbitmq_server_host;
			int port = oneQ.has(QueueConfigKeys.rabbitmq_server_port.getValue()) ? oneQ.getInt(QueueConfigKeys.rabbitmq_server_port.getValue()) : rabbitmq_server_port;
			String exch_name = oneQ.has(QueueConfigKeys.exchange_name.getValue()) ? oneQ.getString(QueueConfigKeys.exchange_name.getValue()) : exchange_name;
			String exch_type = oneQ.has(QueueConfigKeys.exchange_type.getValue()) ? oneQ.getString(QueueConfigKeys.exchange_type.getValue()) : exchange_type;

			qConfigs.add(new QueueConfig(server, port, exch_name, exch_type, oneQ.getString(QueueConfigKeys.queue_name.getValue()), 
					queue_type.valueOf(oneQ.getString(QueueConfigKeys.queue_type.getValue())), 
					oneQ.getString(QueueConfigKeys.queue_updater.getValue()), 
					oneQ.getString(QueueConfigKeys.queue_bind_key.getValue())));
		}
	}

	public String getRabbitmq_server_host() {
		return rabbitmq_server_host;
	}

	public int getRabbitmq_server_port() {
		return rabbitmq_server_port;
	}

	public String getExchange_name() {
		return exchange_name;
	}

	public String getExchange_type() {
		return exchange_type;
	}

	public List<QueueConfig> getqConfigs() {
		return qConfigs;
	}

	public class QueueConfig implements Serializable {

		private static final long serialVersionUID = -6915586104891552313L;

		private String rabbitMQServer;
		private int rabbitMQPort;
		private String exchangeName;
		private String exchangeType;
		private String name;
		private queue_type type;
		private String updater;
		private String bind_key;

		public QueueConfig(String rabbitMQServer, int rabbitMQPort,
				String exchangeName, String exchangeType, String name,
				queue_type type, String updater, String bind_key) {
			this.rabbitMQServer = rabbitMQServer;
			this.rabbitMQPort = rabbitMQPort;
			this.exchangeName = exchangeName;
			this.exchangeType = exchangeType;
			this.name = name;
			this.type = type;
			this.updater = updater;
			this.bind_key = bind_key;
		}

		public String getRabbitMQServer() {
			return rabbitMQServer;
		}
		public void setRabbitMQServer(String rabbitMQServer) {
			this.rabbitMQServer = rabbitMQServer;
		}
		public int getRabbitMQPort() {
			return rabbitMQPort;
		}
		public void setRabbitMQPort(int rabbitMQPort) {
			this.rabbitMQPort = rabbitMQPort;
		}
		public String getExchangeName() {
			return exchangeName;
		}
		public void setExchangeName(String exchangeName) {
			this.exchangeName = exchangeName;
		}
		public String getExchangeType() {
			return exchangeType;
		}
		public void setExchangeType(String exchangeType) {
			this.exchangeType = exchangeType;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public queue_type getType() {
			return type;
		}
		public void setType(queue_type type) {
			this.type = type;
		}
		public String getUpdater() {
			return updater;
		}
		public void setUpdater(String updater) {
			this.updater = updater;
		}
		public String getBind_key() {
			return bind_key;
		}
		public void setBind_key(String bind_key) {
			this.bind_key = bind_key;
		}	
	}
}
