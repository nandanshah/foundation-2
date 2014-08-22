package com.dla.foundation.intelligence.eo.util;

import java.io.Serializable;
import java.util.ArrayList;

import com.dla.foundation.intelligence.eo.filter.Filter;

public class UpdaterConfig implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1628766368443907835L;
	public ArrayList<Filter> filters;

	public UpdaterConfig() {
		filters = new ArrayList<Filter>();
	}
}
