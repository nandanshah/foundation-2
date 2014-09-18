package com.dla.foundation.useritemreco.model;

public class CassandraConfig {

	private String inputKeyspace;
	private String outputKeyspace;
	private String inputColumnfamily;
	private String outputColumnfamily;
	private String pageRowSize;
	private String outputQuery;
	private String inputRpcPort;
	private String inputPartitioner;
	private String outputPartitioner;
	private String inputIpList;
	private String outputIpList;

	public CassandraConfig(String inputKeyspace, String outputKeyspace,
			String inputColumnfamily, String outputColumnfamily,
			String pageRowSize, String outputQuery, String inputRpcPort,
			String inputPartitioner, String outputPartitioner,
			String inputIpList, String outputIpList) {
		super();
		this.inputKeyspace = inputKeyspace;
		this.outputKeyspace = outputKeyspace;
		this.inputColumnfamily = inputColumnfamily;
		this.outputColumnfamily = outputColumnfamily;
		this.pageRowSize = pageRowSize;
		this.outputQuery = outputQuery;
		this.inputRpcPort = inputRpcPort;
		this.inputPartitioner = inputPartitioner;
		this.outputPartitioner = outputPartitioner;
		this.inputIpList = inputIpList;
		this.outputIpList = outputIpList;

	}

	public CassandraConfig(String inputKeyspace, String outputKeyspace,
			String inputColumnfamily, String outputColumnfamily,
			String pageRowSize, String outputQuery) {
		this(inputKeyspace, outputKeyspace, inputColumnfamily,
				outputColumnfamily, pageRowSize, outputQuery, null, null, null,
				null, null);

	}

	public String getInputKeyspace() {
		return inputKeyspace;
	}

	public String getOutputKeyspace() {
		return outputKeyspace;
	}

	public String getInputColumnfamily() {
		return inputColumnfamily;
	}

	public String getOutputColumnfamily() {
		return outputColumnfamily;
	}

	public String getPageRowSize() {
		return pageRowSize;
	}

	public String getOutputQuery() {
		return outputQuery;
	}

	public String getInputRpcPort() {
		return inputRpcPort;
	}

	public String getInputPartitioner() {
		return inputPartitioner;
	}

	public String getOutputPartitioner() {
		return outputPartitioner;
	}

	public String getInputIpList() {
		return inputIpList;
	}

	public String getOutputIpList() {
		return outputIpList;
	}

}
