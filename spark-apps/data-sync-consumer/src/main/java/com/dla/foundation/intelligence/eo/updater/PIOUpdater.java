package com.dla.foundation.intelligence.eo.updater;

import io.prediction.Client;
import io.prediction.FutureAPIResponse;
import io.prediction.UnidentifiedUserException;
import io.prediction.UserActionItemRequestBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;

import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.data.entities.event.Event;
import com.dla.foundation.data.entities.event.EventType;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;
import com.dla.foundation.intelligence.eo.filter.Filter;
import com.dla.foundation.intelligence.eo.filter.FilterException;

/**
 * PredictionIO Specific updater. Used this updater when the data is to be
 * written to PredictionIO.
 * 
 * @author tsudake.psl@dlavideo.com
 * 
 */
public class PIOUpdater extends Updater {

	final Logger logger = Logger.getLogger(this.getClass());
	private String APP_NAME = "pioRecoFetcher";
	private String PROPERTIES_FILE_NAME = "common.properties";
	private String PROPERTIES_FILE_VAR = "commonproperties";
	private String propertiesFilePath = System.getProperty(PROPERTIES_FILE_VAR);
	private final int DEFAULT_API_PORT_NUM = 8000;
	private Client client;

	private PropertiesHandler phandler;
	private String hostname;
	private int port;
	private String appURL;
	private Map<String, String> tenantAppKeyMap;
	private Map<String, Client> tenantClientMap;

	public PIOUpdater() {

		tenantClientMap = new HashMap<String, Client>();
		if (propertiesFilePath == null)
			propertiesFilePath = SparkFiles.get(PROPERTIES_FILE_NAME);

		try {
			phandler = new PropertiesHandler(propertiesFilePath, APP_NAME);
			hostname = phandler.getValue(CommonPropKeys.pio_host);
			try {
				port = (Integer.parseInt(phandler
						.getValue(CommonPropKeys.pio_port)) != -1) ? Integer
								.parseInt(phandler.getValue(CommonPropKeys.pio_port))
								: DEFAULT_API_PORT_NUM;
			} catch (NumberFormatException e) {
				port = DEFAULT_API_PORT_NUM;
				logger.error(e.getMessage(), e);
			}
			appURL = "http://" + hostname + ":" + port;
			tenantAppKeyMap = phandler.getPropMap();
		} catch (IOException e1) {
			logger.error(e1.getMessage(), e1);
		}
	}

	@Override
	protected <TEntity extends SimpleFoundationEntity> TEntity filterEvent(
			TEntity event, ArrayList<Filter> filters) throws FilterException {
		for (Filter filter : filters) {
			event = filter.doFilter(event);
		}
		return event;
	}

	@Override
	protected <TEntity extends SimpleFoundationEntity> TEntity doUpdateSyncEvent(
			TEntity event) {
		// try {
		// client.createUser(event.visitorProfileId);
		// client.createItem(event.customEventValue, new String[]{"movie"});
		// client.userActionItem(event.visitorProfileId,
		// event.customEventAction, event.customEventValue);
		// } catch (ExecutionException | InterruptedException | IOException e) {
		// logger.error(e.getMessage(), e);
		// }
		return null;
	}

	@Override
	protected <TEntity extends SimpleFoundationEntity> void doUpdateAsyncEvent(
			TEntity event) {
		Event pioEvent = (Event) event;
		String tenantId = pioEvent.tenantId;
		EventType eventType = pioEvent.eventType;

		if (!tenantClientMap.containsKey(tenantId)) {
			Client pioClient;
			if ((pioClient = getClient(tenantId)) == null)
				return;
			tenantClientMap.put(tenantId, pioClient);
		}

		client = tenantClientMap.get(tenantId);
		logger.debug("using PIO client for tenantd id: '" + tenantId + "'");

		try {
			if (eventType == EventType.ProfileAdded) {
				client.createUser(pioEvent.visitorProfileId);

				logger.debug(eventType + " event pushed in PIO succesfully");

			} else if (eventType == EventType.ProfileDeleted) {

				client.deleteUser(pioEvent.visitorProfileId);

				logger.debug(eventType + " event pushed in PIO succesfully");

			} else if (eventType == EventType.UserItemPreview) {

				client.userActionItem(pioEvent.visitorProfileId,
						PIOMappingKeys.UserItemPreview.getValue(),
						pioEvent.itemId);

				logger.debug(eventType + " event pushed in PIO succesfully");

			} else if (eventType == EventType.UserItemMoreInfo) {

				client.userActionItem(pioEvent.visitorProfileId,
						PIOMappingKeys.UserItemMoreInfo.getValue(),
						pioEvent.itemId);
				logger.debug(eventType + " event pushed in PIO succesfully");

			} else if (eventType == EventType.UserItemShare) {

				client.userActionItem(pioEvent.visitorProfileId,
						PIOMappingKeys.UserItemShare.getValue(),
						pioEvent.itemId);

				logger.debug(eventType + " event pushed in PIO succesfully");

			} else if (eventType == EventType.UserItemAddToWatchList) {

				client.userActionItem(pioEvent.visitorProfileId,
						PIOMappingKeys.UserItemAddToWatchList.getValue(),
						pioEvent.itemId);

				logger.debug(eventType + " event pushed in PIO succesfully");

			} else if (eventType == EventType.UserItemPlayStart) {

				client.userActionItem(pioEvent.visitorProfileId,
						PIOMappingKeys.UserItemPlayStart.getValue(),
						pioEvent.itemId);

				logger.debug(eventType + " event pushed in PIO succesfully");

			} else if (eventType == EventType.UserItemPurchase) {

				client.userActionItem(pioEvent.visitorProfileId,
						PIOMappingKeys.UserItemPurchase.getValue(),
						pioEvent.itemId);

				logger.debug(eventType + " event pushed in PIO succesfully");

			} else if (eventType == EventType.UserItemRent) {

				client.userActionItem(pioEvent.visitorProfileId,
						PIOMappingKeys.UserItemRent.getValue(), pioEvent.itemId);

				logger.debug(eventType + " event pushed in PIO succesfully");

			} else if (eventType == EventType.UserItemRate) {

				String rating = pioEvent.rateScore;
				client.identify(pioEvent.visitorProfileId);

				try {
					FutureAPIResponse r = client.userActionItemAsFuture(client
							.getUserActionItemRequestBuilder("rate",
									pioEvent.itemId).rate(
											Integer.parseInt(rating)));
					logger.debug("Rate status: " + r.getStatus()
							+ " with message: " + r.getMessage());
				} catch (NumberFormatException e) {
					logger.error(e.getMessage(), e);
				} catch (UnidentifiedUserException e) {
					logger.error(e.getMessage(), e);
				}

				UserActionItemRequestBuilder userActionitemReq = client
						.getUserActionItemRequestBuilder(
								pioEvent.visitorProfileId, "rate", rating);
				userActionitemReq.rate(Integer.parseInt(rating));

				logger.debug(eventType + " event pushed in PIO succesfully");

			} else if (eventType == EventType.UserItemPlayPercentage) {

				String playPercentage = pioEvent.playPercentage;
				if (playPercentage == null
						|| Integer.parseInt(playPercentage) < 70) {

					logger.warn("UserItemPlayPercentage event requires play percenatge >= 70 to push into PIO");

				} else {
					client.userActionItem(pioEvent.visitorProfileId,
							PIOMappingKeys.UserItemPlayPercentage.getValue(),
							pioEvent.itemId);

					logger.debug(eventType + " event pushed in PIO succesfully");
				}

			} else {
				logger.error(eventType
						+ " event is not supported by PIOUpdater.");
			}
		} catch (ExecutionException | InterruptedException | IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public void close() {
		client.close();
		logger.info("PredictionIO Client closed");
	}

	/**
	 * This method intialize PIO client for specified tenant if tenant entry
	 * found in properties.
	 * 
	 * @param tenantId
	 * @return PIO clinet if tenant valid otherwise returns with logging debug
	 *         message.
	 */
	private Client getClient(String tenantId) {
		Client client = null;
		String appKey;

		if (tenantAppKeyMap.containsKey(tenantId)) {
			appKey = tenantAppKeyMap.get(tenantId).split(",")[0];
			client = new Client(appKey, appURL);
		} else {
			logger.error("Tenant entry for '" + tenantId
					+ "' not found in properties. Skipped event handling.");
			return null;
		}
		return client;
	}

	/**
	 * This enum used to Map user action events to PIO supported events.
	 * 
	 * @author dlauser
	 * 
	 */
	private enum PIOMappingKeys {
		UserItemPreview("view"), UserItemMoreInfo("view"), UserItemShare("like"), UserItemAddToWatchList(
				"view"), UserItemPlayStart("view"), UserItemPlayPercentage(
						"view"), UserItemRent("conversion"), UserItemPurchase(
								"conversion");

		private String value;

		PIOMappingKeys(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		@Override
		public String toString() {
			return value;
		}
	}
}