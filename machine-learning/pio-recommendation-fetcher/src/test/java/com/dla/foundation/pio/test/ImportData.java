package com.dla.foundation.pio.test;

import io.prediction.Client;
import io.prediction.FutureAPIResponse;
import io.prediction.UnidentifiedUserException;
import io.prediction.UserActionItemRequestBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.pio.RecommendationFetcherDriver;


public class ImportData implements Serializable {

	private static final long serialVersionUID = -4902237242890409021L;
	private static String itemType = "movie";
	private static PropertiesHandler propertyHandler;
	public static final String DEFAULT_USERLIST_FILE_PATH = "src/main/resources/userlist.csv";
	public static final String PROPERTIES_FILE_VAR = "propertiesfile";
	private static Logger logger = Logger.getLogger(ImportData.class.getName());
	
	

	/**
	 * Instantiates ImportData with instance PropertyHandler
	 * 
	 * @param propertyHandler
	 *            Instance of propertyHandler.
	 */
	public ImportData(PropertiesHandler propertyHandler) {
		ImportData.propertyHandler = propertyHandler;
	}

	/**
	 * Returns list of users from
	 * 
	 * @param fileName
	 * @param separator
	 * @return ArrayList<String> Returns list of users.
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws IOException
	 */

	public ArrayList<String> readUsers(String fileName, String separator)
			throws ExecutionException, InterruptedException, IOException {

		CSVFileHandler fhandler = new CSVFileHandler(fileName, separator);
		String[] elements = null;

		ArrayList<String> userList = new ArrayList<String>();
		while ((elements = fhandler.nextLine()) != null) {

			userList.add(String.valueOf(elements[0]));
			

		}
		return userList;

	}

	private void exportUsersToPIO(Client client, ArrayList<String> userList)
			throws ExecutionException, InterruptedException, IOException {
		for (String user : userList) {
			client.createUser(user);
			
		}
	}

	public void importItems(Client client, String fileName, String separator)
			throws ExecutionException, InterruptedException, IOException {

		CSVFileHandler fhandler = new CSVFileHandler(fileName, separator);
		String[] elements = null;

		while ((elements = fhandler.nextLine()) != null) {

			client.createItem(String.valueOf(elements[0]),
					new String[] { itemType });
			
			}
	}

	public void importUsers(Client client, String fileName, String separator)
			throws ExecutionException, InterruptedException, IOException {
		ArrayList<String> userList = readUsers(fileName, separator);
		exportUsersToPIO(client, userList);

	}

	public void importBehaviorData(Client client, String fileName,
			String separator) throws ExecutionException, InterruptedException,
			IOException {
		CSVFileHandler fhandler = new CSVFileHandler(fileName, separator);
		String[] elements = null;
	
		while ((elements = fhandler.nextLine()) != null) {
			
			if (elements.length < 3)
				continue; // skipping records with
			// less than 3 records
			
			client.userActionItem(String.valueOf(elements[0]), (String.valueOf(elements[1])),
					elements[2]);
			
		}

	}
	public void importRateData(Client client, String fileName, String separator)
			throws IOException, ExecutionException, InterruptedException, NumberFormatException, UnidentifiedUserException {
		CSVFileHandler fhandler = new CSVFileHandler(fileName, separator);
		String[] elements = null;
		
		List<FutureAPIResponse> futureResponse = new ArrayList<FutureAPIResponse>();
		long count =0 ;
		while ((elements = fhandler.nextLine()) != null) {
			count ++;
			if (elements.length < 3)
				continue; // skipping records with
			// less than 3 records
			logger.info("Adding user rating activity as "+(String.valueOf(elements[0])) + " - "
					+ (String.valueOf(elements[1])) + " - "
					+ (String.valueOf(elements[2])));
			
			client.identify(String.valueOf(elements[0]));
			 FutureAPIResponse r = client.userActionItemAsFuture(client.getUserActionItemRequestBuilder("rate", String.valueOf(elements[2])).rate(Integer.parseInt(elements[1])));
			 futureResponse.add(r);
			 UserActionItemRequestBuilder userActionitemReq = client
					.getUserActionItemRequestBuilder(elements[0], "rate",
							elements[2]);
			userActionitemReq.rate((int) Math.round(Double
					.parseDouble(elements[1])));
		}
	 count = 0;
		for (FutureAPIResponse r : futureResponse) {
			count ++;
            if (r.getStatus() != 201) {
                System.err.println(r.getMessage());
            }
        }
			}

}
