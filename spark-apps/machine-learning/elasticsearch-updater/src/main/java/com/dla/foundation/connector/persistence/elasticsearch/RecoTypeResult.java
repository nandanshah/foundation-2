package com.dla.foundation.connector.persistence.elasticsearch;

import com.dla.foundation.connector.model.RecoType;


public class RecoTypeResult {
	

		public String _index;
		public String _type;
		public String _id;
		public long _version;
		public boolean found;
		public boolean exists;
		public RecoType _source;

		@Override
		public String toString() {
			return "RecoTypeResult [_id=" + _id + ", _index=" + _index
					+ ", _source=" + _source.toString() + ", _type=" + _type + ", _version="
					+ _version +", exists=" + exists+ ", found=" + found + "]";
		}
		
		
	
}
