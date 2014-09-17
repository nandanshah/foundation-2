package com.dla.foundation.socialReco.model;
import java.io.Serializable;
import java.util.List;

public class UserFrndId implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public  static String DELIM = "####";
	private String profileId;
	private List<String> friendId;
	
	
	public UserFrndId(String profileid, List friendid)
	{
		this.profileId = profileid;
		this.friendId = friendid;
	}
	
	public UserFrndId()
	{
		super();
	}

	public void setProfileid(String profileid) {
		this.profileId = profileid;
	}
	public void setFriendid(List friendid) {
		this.friendId = friendid;
	}

	public String getProfileId() {
		return profileId;
	}
	public List getFriendId() {
		return friendId;
	}
	
	@Override
	public String toString() {
		return getProfileId()+DELIM+getFriendId();
	}
}
