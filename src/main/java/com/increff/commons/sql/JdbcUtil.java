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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class JdbcUtil {
	
	protected static void closeQuitely(Statement stmt) {
		if (stmt == null) {
			return;
		}
		try {
			stmt.close();
		} catch (SQLException e) {
		}
	}

	protected static HashMap<String, Integer> getColumMap(ResultSet rs) throws SQLException {
		ResultSetMetaData rsm = rs.getMetaData();
		int columnCount = rsm.getColumnCount();
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		for (int i = 1; i <= columnCount; i++) {
			map.put(rsm.getColumnName(i), i);
		}
		return map;
	}

	protected static ResultRow getResultRow(ResultSet rs, HashMap<String, Integer> columnMap) throws SQLException {
		ResultSetMetaData rsm = rs.getMetaData();
		int columnCount = rsm.getColumnCount();
		String[] tokens = new String[columnCount];
		for (int i = 1; i <= columnCount; i++) {
			tokens[i-1] = rs.getString(i);
		}
		ResultRow rr = new ResultRow(columnMap);
		rr.setTokens(tokens);
		return rr;
	}

	protected static void closeQuietly(AutoCloseable c) {
		if (c == null) {
			return;
		}
		try {
			c.close();
		} catch (Exception e) {

		}
	}

}
