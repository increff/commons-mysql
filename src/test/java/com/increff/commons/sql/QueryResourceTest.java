/*
 * Copyright (c) 2021. Increff
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.increff.commons.sql;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class QueryResourceTest {

	private static String resource = "/com/increff/commons/mysql/multi-query.sql";

	@Test
	public void testGetQueryWithProperties() throws DbException {
		Properties props = new Properties();
		props.put("key1", "value1");
		props.put("key2", "value2");
		props.put("key3", "value3");

		QueryResource queryResource = new QueryResource("");
		String query01 = queryResource.getQuery("query01.txt", props);
		assertEquals("SELECT value1 FROM value2 WHERE value3 = 100;", query01);
	}

	@Test
	public void testGetQueryWithHashMap() throws DbException {
		HashMap<String, String> map = new HashMap<>();
		map.put("key1", "value1");
		map.put("key2", "value2");
		map.put("key3", "value3");

		QueryResource queryResource = new QueryResource("");
		String query01 = queryResource.getQuery("query01.txt", map);
		assertEquals("SELECT value1 FROM value2 WHERE value3 = 100;", query01);
	}

	@Test
	public void testGetQueryWithMismatchingKeys() throws DbException {
		HashMap<String, String> map = new HashMap<>();
		map.put("key10", "value1");
		map.put("key20", "value2");
		map.put("key30", "value3");

		QueryResource queryResource = new QueryResource("");
		String query01 = queryResource.getQuery("query01.txt", map);
		assertEquals("SELECT ${key1} FROM ${key2} WHERE ${key3} = 100;", query01);
	}

	@Test
	public void testGetQueryWithRepeatingVariablesInQuery() throws DbException {
		HashMap<String, String> map = new HashMap<>();
		map.put("key1", "value1");
		map.put("key2", "value2");

		QueryResource queryResource = new QueryResource("");
		String query01 = queryResource.getQuery("query02.txt", map);
		assertEquals("SELECT value1 FROM value1 WHERE value2 = value2;", query01);
	}

	@Test
	public void testGetQueryWithInvalidQuerySource() throws DbException {
		HashMap<String, String> map = new HashMap<>();
		QueryResource queryResource = new QueryResource("");

		try {
			queryResource.getQuery("queryINVALID.txt", map);
		} catch (DbException e){
			assertTrue(true);
		}
	}

	@Test
	public void testCommentFilter() throws IOException {
		String s = IOUtils.resourceToString(resource, Charset.forName("utf-8"));
		String cleanedStr = QueryResource.cleanComments(s);
		OperatingSystem os = OperatingSystem.getOs();
		switch (os) {
		case linux:
			assertEquals(879, cleanedStr.length());			
			break;
		case windows:
			assertEquals(896, cleanedStr.length());
			break;
		default:
			fail("Invalid operating system: " + os.toString());
			break;
		}
	}

}
