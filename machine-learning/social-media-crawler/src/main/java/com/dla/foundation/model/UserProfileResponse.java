package com.dla.foundation.model;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UserProfileResponse {
	public final String UID;
	public final String UIDSig;
	public final String timestamp;
	public final String UIDSignature;
	public final String signatureTimestamp;
	public final boolean isSiteUser;
	public final boolean isConnected;
	public final boolean isLoggedIn;
	public final boolean isTempUser;
	public final String loginProvider;
	public final String loginProviderUID;
	public final boolean isSiteUID;
	public final String nickname;
	public final String photoURL;
	public final String thumbnailURL;
	public final String firstName;
	public final String lastName;
	public final String gender;
	public final String age;
	public final String birthDay;
	public final String birthMonth;
	public final String birthYear;
	public final String email;
	public final String country;
	public final String state;
	public final String profileURL;
	public final String proxiedEmail;
	public final String capabilities;
	public final String providers;
	public final int statusCode;
	public final int errorCode;
	public final String statusReason;
	public final String bio;
	public final String locale;
	public final String religion;
	public final long timezone;
	public final String address;
	public final String honors;
	public final String industry;
	public final String specialties;
	public final CompanyDetails work[];
	public final Identities identities[];
	public final Education education[];
	public final Favorites favorites;
	public final Name certifications[];
	public final TitleDetails patents[];
	public final TitleDetails publications[];
	public final Skill skills[];

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Skill {
		public final String skill;

		public Skill() {
			this(null);
		}

		public Skill(String skill) {
			this.skill = skill;
		}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class TitleDetails {
		public final String title;
		public final FBDate date;

		public TitleDetails() {
			this(null, null);
		}

		public TitleDetails(String title, FBDate date) {
			this.title = title;
			this.date = date;
		}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class FBDate {
		public final String date;
		public final String month;
		public final String year;

		public FBDate() {
			this(null, null, null);
		}

		public FBDate(String date, String month, String year) {
			this.date = date;
			this.month = month;
			this.year = year;
		}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Name {
		public final String name;

		public Name() {
			this(null);
		}

		public Name(String name) {
			this.name = name;
		}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Identities {
		public final String provider;
		public final String providerUID;
		public final boolean isLoginIdentity;
		public final String nickname;
		public final String photoURL;
		public final String thumbnailURL;
		public final String firstName;
		public final String lastName;
		public final String gender;
		public final String age;
		public final String birthDay;
		public final String birthMonth;
		public final String birthYear;
		public final String email;
		public final String city;
		public final String profileURL;
		public final String proxiedEmail;
		public final boolean allowsLogin;
		public final boolean isExpiredSession;
		public final String bio;
		public final String languages;
		public final String locale;
		public final String religion;
		public final String timezone;

		public final long lastLoginTime;
		public final String lastUpdated;
		public final long lastUpdatedTimestamp;
		public final String oldestDataUpdated;
		public final long oldestDataUpdatedTimestamp;
		public final Hometown hometown;
		public final Details likes[];
		public final CompanyDetails work[];
		public final Education education[];
		public final Favorites favorites[];

		public Identities() {
			this(null, null, false, null, null, null, null, null, null, null,
					null, null, null, null, null, null, null, false, false,
					null, null, null, null, null, 0, null, 0, null, 0, null,
					null, null, null, null);
		}

		public Identities(String provider, String providerUID,
				boolean isLoginIdentity, String nickname, String photoURL,
				String thumbnailURL, String firstName, String lastName,
				String gender, String age, String birthDay, String birthMonth,
				String birthYear, String email, String city, String profileURL,
				String proxiedEmail, boolean allowsLogin,
				boolean isExpiredSession, String bio, String languages,
				String locale, String religion, String timezone,
				long lastLoginTime, String lastUpdated,
				long lastUpdatedTimestamp, String oldestDataUpdated,
				long oldestDataUpdatedTimestamp, Hometown hometown,
				Details[] likes, CompanyDetails[] work, Education[] education,
				Favorites[] favorites) {
			super();
			this.provider = provider;
			this.providerUID = providerUID;
			this.isLoginIdentity = isLoginIdentity;
			this.nickname = nickname;
			this.photoURL = photoURL;
			this.thumbnailURL = thumbnailURL;
			this.firstName = firstName;
			this.lastName = lastName;
			this.gender = gender;
			this.age = age;
			this.birthDay = birthDay;
			this.birthMonth = birthMonth;
			this.birthYear = birthYear;
			this.email = email;
			this.city = city;
			this.profileURL = profileURL;
			this.proxiedEmail = proxiedEmail;
			this.allowsLogin = allowsLogin;
			this.isExpiredSession = isExpiredSession;
			this.bio = bio;
			this.languages = languages;
			this.locale = locale;
			this.religion = religion;
			this.timezone = timezone;
			this.lastLoginTime = lastLoginTime;
			this.lastUpdated = lastUpdated;
			this.lastUpdatedTimestamp = lastUpdatedTimestamp;
			this.oldestDataUpdated = oldestDataUpdated;
			this.oldestDataUpdatedTimestamp = oldestDataUpdatedTimestamp;
			this.hometown = hometown;
			this.likes = likes;
			this.work = work;
			this.education = education;
			this.favorites = favorites;
		}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Education {
		public final String school;
		public final String schoolType;
		public final String startYear;

		public Education() {
			this(null, null, null);
		}

		public Education(String school, String schoolType, String startYear) {
			this.school = school;
			this.schoolType = schoolType;
			this.startYear = startYear;
		}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Favorites {
		public final Details interests[];
		public final Details activities[];
		public final Details books[];
		public final Details movies[];
		public final Details television[];

		public Favorites() {
			this(null, null, null, null, null);
		}

		public Favorites(Details[] interests, Details[] activities,
				Details[] books, Details[] movies, Details[] television) {
			this.interests = interests;
			this.activities = activities;
			this.books = books;
			this.movies = movies;
			this.television = television;
		}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Details {
		public final String name;
		public final String category;

		public Details() {
			this(null, null);
		}

		public Details(String name, String category) {
			this.name = name;
			this.category = category;
		}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Hometown {
		public final String id;
		public final String name;

		public Hometown() {
			this(null, null);
		}

		public Hometown(String id, String name) {
			this.id = id;
			this.name = name;
		}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class CompanyDetails {
		public final String company;
		public final String companyID;
		public final String title;
		public final String startDate;
		public final String industry;
		public final boolean isCurrent;

		public CompanyDetails() {
			this(null, null, null, null, null, false);
		}

		public CompanyDetails(String company, String companyID, String title,
				String startDate, String industry, boolean isCurrent) {
			super();
			this.company = company;
			this.companyID = companyID;
			this.title = title;
			this.startDate = startDate;
			this.industry = industry;
			this.isCurrent = isCurrent;
		}

	}

	public UserProfileResponse() {
		this(null, null, null, null, null, false, false, false, false, null,
				null, false, null, null, null, null, null, null, null, null,
				null, null, null, null, null, null, null, null, null, -1, -1,
				null, null, null, null, (long) 0.0, null, null, null, null,
				null, null, null, null, null, null, null, null);
	}

	public UserProfileResponse(String uID, String uIDSig, String timestamp,
			String uIDSignature, String signatureTimestamp, boolean isSiteUser,
			boolean isConnected, boolean isLoggedIn, boolean isTempUser,
			String loginProvider, String loginProviderUID, boolean isSiteUID,
			String nickname, String photoURL, String thumbnailURL,
			String firstName, String lastName, String gender, String age,
			String birthDay, String birthMonth, String birthYear, String email,
			String country, String state, String profileURL,
			String proxiedEmail, String capabilities, String providers,
			int statusCode, int errorCode, String statusReason, String bio,
			String locale, String religion, long timezone, String address,
			String honors, String industry, String specialties,
			CompanyDetails[] work, Identities[] identities,
			Education[] education, Favorites favorites, Name[] certifications,
			TitleDetails[] patents, TitleDetails[] publications, Skill[] skills) {
		UID = uID;
		UIDSig = uIDSig;
		this.timestamp = timestamp;
		UIDSignature = uIDSignature;
		this.signatureTimestamp = signatureTimestamp;
		this.isSiteUser = isSiteUser;
		this.isConnected = isConnected;
		this.isLoggedIn = isLoggedIn;
		this.isTempUser = isTempUser;
		this.loginProvider = loginProvider;
		this.loginProviderUID = loginProviderUID;
		this.isSiteUID = isSiteUID;
		this.nickname = nickname;
		this.photoURL = photoURL;
		this.thumbnailURL = thumbnailURL;
		this.firstName = firstName;
		this.lastName = lastName;
		this.gender = gender;
		this.age = age;
		this.birthDay = birthDay;
		this.birthMonth = birthMonth;
		this.birthYear = birthYear;
		this.email = email;
		this.country = country;
		this.state = state;
		this.profileURL = profileURL;
		this.proxiedEmail = proxiedEmail;
		this.capabilities = capabilities;
		this.providers = providers;
		this.statusCode = statusCode;
		this.errorCode = errorCode;
		this.statusReason = statusReason;
		this.bio = bio;
		this.locale = locale;
		this.religion = religion;
		this.timezone = timezone;
		this.address = address;
		this.honors = honors;
		this.industry = industry;
		this.specialties = specialties;
		this.work = work;
		this.identities = identities;
		this.education = education;
		this.favorites = favorites;
		this.certifications = certifications;
		this.patents = patents;
		this.publications = publications;
		this.skills = skills;
	}

}