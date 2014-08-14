package com.dla.foundation.fis.eo.dispatcher;

import java.io.Closeable;
import java.io.IOException;

import com.dla.foundation.fis.eo.entities.Event;
import com.dla.foundation.fis.eo.exception.DispatcherException;

/**
 * This interface will provide the methods open for web service developer
 * 
 * @author shishir_shivhare
 * 
 */
public interface EODispatcher extends Closeable {

	public void init(EOConfig eoConfig) throws IOException;
	public boolean dispatchAsync(Event e) throws DispatcherException;
	public Event dispatchSync(Event e) throws DispatcherException;
	public void close() throws IOException;
}
