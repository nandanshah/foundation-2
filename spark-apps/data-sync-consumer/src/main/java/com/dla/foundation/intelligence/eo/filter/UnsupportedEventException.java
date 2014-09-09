package com.dla.foundation.intelligence.eo.filter;

public class UnsupportedEventException extends FilterException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5041922118396284445L;
	private final static String msg = "Input event type is not supported by updater."; 
	
	public UnsupportedEventException() {
		super(msg);
	}
	
	public UnsupportedEventException(String msg) {
		super(msg);
	}
}
