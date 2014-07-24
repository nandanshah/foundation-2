package com.dla.foundation.services.queue.updater;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.net.ssl.HttpsURLConnection;
import org.apache.log4j.Logger;
import scala.NotImplementedError;
import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;

public class EmailUpdater implements Updater {

	final Logger logger = Logger.getLogger(this.getClass());

	@Override
	public AnalyticsCollectionEvent updateSyncEvent(
			AnalyticsCollectionEvent event) {
		String emailContent = event.customEventValue;
		String providerUrl = event.customEventAction;
		Callable<String> emailWorker = new EmailWorker(emailContent,
				providerUrl);
		ExecutorService exService = Executors
				.newSingleThreadScheduledExecutor();
		Future<String> future = exService.submit(emailWorker);
		try {
			event.customEventLabel = future.get();
		} catch (InterruptedException e) {
			logger.error("Error receiving response from Mandrill Service");
			e.printStackTrace();
		} catch (ExecutionException e) {
			logger.error("Error receiving response from Mandrill Service");
			e.printStackTrace();
		}
		logger.info("Email status: " + event.customEventLabel);
		return event;
	}

	@Override
	public void updateAsyncEvent(AnalyticsCollectionEvent event) {
		throw new NotImplementedError(
				"Async event not supported in Email Service");
	}

	@Override
	public void close() {

	}

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
				e.printStackTrace();
			} finally {
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

}
