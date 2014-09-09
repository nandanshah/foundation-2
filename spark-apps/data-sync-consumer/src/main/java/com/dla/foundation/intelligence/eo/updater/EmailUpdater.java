package com.dla.foundation.intelligence.eo.updater;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.net.ssl.HttpsURLConnection;

import org.apache.log4j.Logger;

import scala.NotImplementedError;

import com.dla.foundation.intelligence.eo.entity.UserEvent;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;
import com.dla.foundation.intelligence.eo.filter.Filter;
import com.dla.foundation.intelligence.eo.filter.FilterException;

/**
 * 
 * @author ashish belokar
 * 
 *         Updater module for Email messages. Forwards the messages to the
 *         EmailWorker class. Custom labels from the AnalyticsCollectionEvent
 *         are used since there are no specific fields for email service.
 * 
 */
public class EmailUpdater extends Updater {

	final Logger logger = Logger.getLogger(this.getClass());

	/**
	 * This class forwards the email message to the Mandrill email service.
	 * This class is moved as it is from the platform project since email sending funtionality is moved here.
	 */
	private class EmailWorker implements Callable<String> {

		String emailMsgContent;
		String emailProviderUrl;

		public EmailWorker(String message, String url) {
			this.emailMsgContent = message;
			this.emailProviderUrl = url;
		}

		@Override
		public String call() {
			HttpsURLConnection conn = null;
			StringBuilder sb = null;
			BufferedReader br = null;
			try {
				URL url = new URL(emailProviderUrl);
				conn = (HttpsURLConnection) url.openConnection();
				conn.setRequestMethod("POST");
				conn.setRequestProperty("Content-Type", "application/json");
				conn.setRequestProperty("Accept", "application/json");
				conn.setDoOutput(true);
				OutputStream os = conn.getOutputStream();
				os.write(emailMsgContent.getBytes());
				os.flush();

				sb = new StringBuilder();
				br = new BufferedReader(new InputStreamReader(
						(conn.getInputStream())));

				String output;

				while ((output = br.readLine()) != null) {
					sb.append(output);
				}
			} catch (IOException e) {
				if (conn != null) {
					conn.disconnect();
				}
				if (br != null) {
					try {
						br.close();
					} catch (IOException ioException) {
						ioException.printStackTrace();
					}
				}
			}
			return sb.toString();

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
		//		String emailContent = event.customEventValue;
		//		String providerUrl = event.customEventAction;
		//		Callable<String> emailWorker = new EmailWorker(emailContent,
		//				providerUrl);
		//		ExecutorService exService = Executors
		//				.newSingleThreadScheduledExecutor();
		//		Future<String> future = exService.submit(emailWorker);
		//		try {
		//			event.customEventLabel = future.get();
		//		} catch (InterruptedException e) {
		//			logger.error(e.getMessage(), e);
		//		} catch (ExecutionException e) {
		//			logger.error(e.getMessage(), e);
		//		}
		//		logger.info("Email status: " + event.customEventLabel);
		return event;
	}

	@Override
	protected <TEntity extends SimpleFoundationEntity> void doUpdateAsyncEvent(
			TEntity event) {
		throw new NotImplementedError(
				"Async event not supported in Email Service");
	}

	@Override
	public void close() {

	}
}
