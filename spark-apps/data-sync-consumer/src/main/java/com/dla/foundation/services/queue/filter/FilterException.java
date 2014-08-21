package com.dla.foundation.services.queue.filter;

public class FilterException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2270831249036172019L;

	public FilterException() {
		super("Event failed to pass through filter");
	}
	
	public FilterException(String msg) {
		super(msg);
	}
}
