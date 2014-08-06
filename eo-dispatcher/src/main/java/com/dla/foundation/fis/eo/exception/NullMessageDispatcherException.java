package com.dla.foundation.fis.eo.exception;

public class NullMessageDispatcherException extends DispatcherException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2693638862938554875L;

	public NullMessageDispatcherException() {
		super("Message to be dispatched is null");
	}
	
	public NullMessageDispatcherException(String message) {
		super("Message to be dispatched is null");
	}
}
