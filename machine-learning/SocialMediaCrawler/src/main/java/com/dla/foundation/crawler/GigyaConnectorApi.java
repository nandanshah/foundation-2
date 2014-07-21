package com.dla.foundation.crawler;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;

import org.apache.log4j.Logger;
//import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.dla.foundation.model.FriendsInfoResponse;
import com.dla.foundation.model.UserProfileResponse;
import com.gigya.socialize.GSException;
import com.gigya.socialize.GSRequest;
import com.gigya.socialize.GSResponse;

public class GigyaConnectorApi implements Serializable {

	private static final long serialVersionUID = -9037726580504436036L;

	final private String _secretKey;
	final private String _apiKey;
	@SuppressWarnings("unused")
	final private String _apiScheme;
	@SuppressWarnings("unused")
	final private String _apiDomain;

	private static Logger logger = Logger.getLogger(GigyaConnectorApi.class);

	public static final int SUCCESS_CODE = 0;
	public static final int TIMEOUT_MILLIS = 60000;

	public GigyaConnectorApi(String apiKey, String secretKey, String apiScheme,
			String apiDomain) {
		_apiKey = apiKey;
		_secretKey = secretKey;
		_apiScheme = apiScheme;
		_apiDomain = apiDomain;

	}

	final public class Operations {
		final public static String socialize_getFriendsInfo = "socialize.getFriendsInfo";
		final public static String socialize_getUserInfo = "socialize.getUserInfo";
	}

	final public class Providers {
		final public static String facebook = "facebook";
		final public static String twitter = "twitter";
		final public static String googleplus = "googleplus";
	}

	final public class Methods {
		final public static String GET = "GET";
		final public static String POST = "POST";
	}

	public static class GetUserTokenResponse {
		public String access_token;
		public String state;
		public String error;
		public String error_description;
	}

	public final static class Parameters {
		public final static String uid = "UID";
		public final static String format = "format";
		public final static String siteOnlyUsers = "siteUsersOnly";
	}

	public FriendsInfoResponse getFriends(String uid, boolean siteOnlyUsers)
			throws IOException, URISyntaxException, GSException {
		return getFriends(uid, siteOnlyUsers, TIMEOUT_MILLIS);
	}

	/**
	 * Calls Gigya to get friends information. SiteOnlyUsers set to true returns
	 * friends which have registered through the application.
	 * 
	 * @param uid
	 * @return Json String containing friends information
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws GSException
	 */
	public FriendsInfoResponse getFriends(String uid, boolean siteOnlyUsers,
			int timeout) throws IOException, URISyntaxException, GSException {

		GSRequest request = new GSRequest(_apiKey, _secretKey,
				Operations.socialize_getFriendsInfo, false);

		request.setParam(Parameters.uid, uid);
		request.setParam(Parameters.format, "json");
		if (siteOnlyUsers)
			request.setParam(Parameters.siteOnlyUsers, true);

		GSResponse response = request.send(timeout);

		int errorCode = response.getErrorCode();
		if (errorCode != 0) {
			logger.error(response.getErrorDetails());
			throw new GSException(response.getErrorMessage());
		}

		String response_json = response.getResponseText();
		ObjectMapper mapper = new ObjectMapper();
		FriendsInfoResponse friendsInfoResponse = mapper.readValue(
				response_json, FriendsInfoResponse.class);
		return friendsInfoResponse;
	}

	public UserProfileResponse getUserInfo(String uid) throws IOException,
			URISyntaxException, GSException {
		return getUserInfo(uid, TIMEOUT_MILLIS);
	}

	/**
	 * Calls Gigya API to get User Profile information.
	 * 
	 * @param uid
	 * @return Json String containing profile information
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws GSException
	 */
	public UserProfileResponse getUserInfo(String uid, int timeout)
			throws IOException, URISyntaxException, GSException {

		GSRequest request = new GSRequest(_apiKey, _secretKey,
				Operations.socialize_getUserInfo, false);

		request.setParam(Parameters.uid, uid);
		request.setParam(Parameters.format, "json");

		GSResponse response = request.send(timeout);
		int errorCode = response.getErrorCode();
		if (errorCode != 0) {
			logger.error(response.getErrorDetails());
			throw new GSException(response.getErrorMessage());
		}

		String response_json = response.getResponseText();
		ObjectMapper mapper = new ObjectMapper();
		UserProfileResponse userProfileResponse = mapper.readValue(
				response_json, UserProfileResponse.class);
		return userProfileResponse;
	}

}
