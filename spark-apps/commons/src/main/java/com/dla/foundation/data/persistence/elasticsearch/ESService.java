package com.dla.foundation.data.persistence.elasticsearch;

import java.io.IOException;

import com.dla.foundation.data.entities.event.Event;

public interface ESService {
			
/*	void addItem(Event event) throws IOException;
	void deleteItem(Event event) throws IOException;
	void updateItem(Event event) throws IOException;*/
	void postBulkEvents(String bulkevents);
	void deleteUserReco(String bulkevents);
}
