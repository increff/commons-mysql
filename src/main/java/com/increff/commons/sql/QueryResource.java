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
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;

import com.nextscm.commons.lang.CollectionUtil;
import com.nextscm.commons.lang.FileUtil;

public class QueryResource {

	private String resourceBase;

	public QueryResource(String resourceBase) {
		this.resourceBase = resourceBase;
	}

	/**
	 * Returns a fully composed query by substituting the variables from given Properties object
	 * Variables must be in the format ${variable_name}
	 * @param queryName Name of resource in resourceBase location which stores the desired query
	 * @param props Properties object
	 * @return Query string with variables substituted
	 */
	public String getQuery(String queryName, Properties props) throws DbException {
		Map<String, String> data = CollectionUtil.toMap(props);
		return getQuery(queryName, data);
	}

	/**
	 * Returns a fully composed query by substituting the variables from given variable mapping
	 * Variables must be in the format ${variable_name}
	 * @param queryName Name of resource in resourceBase location which stores the desired query
	 * @param data Variable name to variable value mapping
	 * @return Query string with variables substituted=
	 */
	public String getQuery(String queryName, Map<String, String> data) throws DbException {
		String query = getQuery(queryName);
		return StringSubstitutor.replace(query, data);
	}

	/**
	 * Fetches the query string stored in resourceBase with the specified identifier
	 * @param resourceName Name of query storing resource
	 * @return Fetched query value
	 */
	public String getQuery(String resourceName) throws DbException {
		String resourcePath = resourceBase + "/" + resourceName;
		InputStream is = this.getClass().getResourceAsStream(resourcePath);
		if (is == null) {
			throw new DbException("Query resource not found: " + resourcePath);
		}
		String query;
		try {
			query = IOUtils.toString(is, "utf-8");
		} catch (IOException e) {
			throw new DbException("Error reading resource: " + resourceName);
		}
		FileUtil.closeQuietly(is);
		return cleanComments(query);
	}

	/**
	 * Removes SQL comments from SQL queries. (?m) turns on the line mode
	 * @param s Query strong from which to remove comments
	 * @return Cleaned query string
	 */
	public static String cleanComments(String s) {
		String clean = s.replaceAll("(?m)#.*$", ""); // matches #comment
		clean = clean.replaceAll("(?m)--.*$", ""); // matches --comment
		clean = clean.replaceAll("(?m)//.*$", "");// matches //comment
		clean = clean.replaceAll("(?m)/\\*.*$", "");// matches /*comment
		return clean;
	}
}