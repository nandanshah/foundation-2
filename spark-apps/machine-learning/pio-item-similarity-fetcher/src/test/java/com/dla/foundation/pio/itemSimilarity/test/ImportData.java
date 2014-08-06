package com.dla.foundation.pio.itemSimilarity.test;

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

import com.dla.foundation.pio.itemSimilarity.test.CSVFileHandler;
import com.dla.foundation.pio.util.PropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;

public class ImportData implements Serializable {
	public static final String DEFAULT_USERS_FILE_PATH = "src/test/resources/TestData/movieDataUser.csv";
	public static final String DEFAULT_PROPERTIES_FILE_PATH = "src/test/resources/PIO_props.properties";
	public static final String DEFAULT_ITEMS_FILE_PATH = "src/test/resources/movieData.csv";
	public static final String DEFAULT_BEHAVIOUR_FILE_PATH = "src/test/resources/activity.csv";
	public static final String DEFAULT_PREFETCHED_USER_FILE_PATH = "src/test/resources/TestData/movieDataUser.csv";
	

	private static final long serialVersionUID = -4902237242890409021L;
	private static String itemType = "movie";
	private static PropertiesHandler propertyHandler;
	private static Logger logger = Logger
			.getLogger(ImportData.class.getClass());
	public static final String PROPERTIES_FILE_VAR = "propertiesfile";

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
			System.out.println("List add " + String.valueOf(elements[0]));

		}
		return userList;

	}

	private void exportUsersToPIO(Client client, ArrayList<String> userList)
			throws ExecutionException, InterruptedException, IOException {
		for (String user : userList) {
			client.createUser(user);
			System.out.println(user + "user added to PIO");
		}
	}

	public void importItems(Client client, String fileName, String separator)
			throws ExecutionException, InterruptedException, IOException {

		CSVFileHandler fhandler = new CSVFileHandler(fileName, separator);
		String[] elements = null;

		while ((elements = fhandler.nextLine()) != null) {

			client.createItem(String.valueOf(elements[0]),
					new String[] { itemType });
			System.out.println(String.valueOf(elements[0]) + " movie added ");
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
		long count = 0;
		while ((elements = fhandler.nextLine()) != null) {
			count++;
			if (elements.length < 3)
				continue; // skipping records with
			// less than 3 records
			System.out.println(count + " " + (String.valueOf(elements[0]))
					+ " - " + (String.valueOf(elements[1])) + " - "
					+ (String.valueOf(elements[2])));
			client.userActionItem(String.valueOf(elements[0]),
					(String.valueOf(elements[1])), elements[2]);
			// throw new IOException();
		}

	}

	public void importRateData(Client client, String fileName, String separator)
			throws IOException, ExecutionException, InterruptedException,
			NumberFormatException, UnidentifiedUserException {
		CSVFileHandler fhandler = new CSVFileHandler(fileName, separator);
		String[] elements = null;

		List<FutureAPIResponse> futureResponse = new ArrayList<FutureAPIResponse>();
		long count = 0;
		while ((elements = fhandler.nextLine()) != null) {
			count++;
			if (elements.length < 3)
				continue; // skipping records with
			// less than 3 records
			System.out.println(count + " " + (String.valueOf(elements[0]))
					+ " - " + (String.valueOf(elements[1])) + " - "
					+ (String.valueOf(elements[2])));

			client.identify(String.valueOf(elements[0]));
			FutureAPIResponse r = client.userActionItemAsFuture(client
					.getUserActionItemRequestBuilder("rate",
							String.valueOf(elements[2])).rate(
							Integer.parseInt(elements[1])));
			futureResponse.add(r);
			UserActionItemRequestBuilder userActionitemReq = client
					.getUserActionItemRequestBuilder(elements[0], "rate",
							elements[2]);
			userActionitemReq.rate((int) Math.round(Double
					.parseDouble(elements[1])));
		}
		count = 0;
		for (FutureAPIResponse r : futureResponse) {
			count++;
			if (r.getStatus() != 201) {
				System.err.println(r.getMessage());
			} else {
				System.out.println(count + " Done : " + r.getMessage());
			}
		}
	}

}
