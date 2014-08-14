package com.dla.foundation.fis.eo.dispatcher;

import java.util.List;

import com.dla.foundation.fis.eo.entities.Event;
import com.dla.foundation.fis.eo.exception.DispatcherException;

/**
 * This class will call rabbitmq push event
 * 
 * @author shishir_shivhare
 *
 */
public class RabbitMQDispatcher extends AbstractRMQDispatcher<Event> {

	private EventRouteProvider er;
	
	public RabbitMQDispatcher() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException {
		this.conf = new EOConfig();
		er = new EventRouteProvider();
	}

	@Override
	public boolean dispatchAsync(Event e) throws DispatcherException {
		List<String> eventRoutes = er.getRoute(e.hitType); 
		for (String route : eventRoutes) {
			enqueueAsync(e, route);
		}
		return true;
	}

	@Override
	public Event dispatchSync(Event e) throws DispatcherException {
		return null;
	}
}