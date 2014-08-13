package com.dla.foundation.connector.persistence.elasticsearch;

import com.dla.foundation.connector.model.UserRecommendation;


public class UserRecoResult {

	public String _index;
	public String _type;
	public String _id;
	public long _version;
	public boolean found;
	
	public UserRecommendation _source;

	@Override
	public String toString() {
		return "UserRecoResult [_id=" + _id + ", _index=" + _index
				+ ", _source=" + _source.toString() + ", _type=" + _type + ", _version="
				+ _version + ", found=" + found + "]";
	}
	
	
}
