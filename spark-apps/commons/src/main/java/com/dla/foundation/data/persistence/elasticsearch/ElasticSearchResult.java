package com.dla.foundation.data.persistence.elasticsearch;

public class ElasticSearchResult {
	//{"ok":true,"_index":"tim","_type":"stuff","_id":"1","created"true}
	public boolean ok;
	public String _index;
	public String _type;
	public String _id;
	public String _version;
	public boolean created;
	public boolean found;
}
