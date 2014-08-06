package com.dla.foundation.data.persistence.elasticsearch;

import com.dla.foundation.services.contentdiscovery.entities.MediaItem;

public class MovieItemResult {
	public String _index;
	public String _type;
	public String _id;
	public String _version;
	public boolean found;
	public MediaItem _source;

}
