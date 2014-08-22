package com.dla.foundation.fis.eo.dispatcher;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class EventRouteProvider {

	private HashMap<String, List<String>> routeMap = new HashMap<String, List<String>>();
	private List<String> allRoutes = new ArrayList<String>();

	public EventRouteProvider() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException {
		Class c = Class.forName("com.dla.foundation.fis.eo.dispatcher.RabbitMQDispatcherConstants");
		Field[] f = c.getFields();
		for (Field field : f) {
			allRoutes.add(field.get(this).toString());
		}
	}

	public List<String> getRoute(String eventType) {
		if(routeMap.containsKey(eventType)) {
			return routeMap.get(eventType);
		} else {
			List<String> tmp = new ArrayList<String>();
			for (String route : allRoutes) {
				if(route.startsWith(eventType)) {
					tmp.add(route.trim());
				}
			}
			routeMap.put(eventType, tmp);
			return tmp;
		}
	}
}
