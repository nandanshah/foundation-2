package com.dla.foundation.analytics.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;

public class CommonPropertiesTest {

	private PropertiesHandler handler;

	@Before
	public void before() {
		try {
			handler = new PropertiesHandler("src/main/resources/local/common.properties");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testCommonPropWithPath() {
		try {
			final String cs_entityPackagePrefix = "com.dla.foundation";
			assertEquals(cs_entityPackagePrefix,
					handler.getValue(CommonPropKeys.cs_entityPackagePrefix));
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
}
