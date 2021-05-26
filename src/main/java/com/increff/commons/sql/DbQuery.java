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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * WARNING: This class uses TSV format for files. It automatically applies
 * <b>.tsv</b> to the given file names
 */
public class DbQuery {

	private Connection con;
	private String jdbcUrl;
	private String username;
	private String password;

	/**
	 * Set up JDBC Driver for interacting with a database
	 * @param jdbcDriver Name of JDBC driver class
	 * @param jdbcUrl URL for JDBC driver
	 * @param username Username for JDBC
	 * @param password Password for JDBC
	 */
	public DbQuery(String jdbcDriver, String jdbcUrl, String username, String password) throws DbException {
		this.jdbcUrl = jdbcUrl;
		this.username = username;
		this.password = password;
		try {
			Class.forName(jdbcDriver);
		} catch (ClassNotFoundException e) {
			throw new DbException("Error loading JDBC driver", e);
		}
	}

	/**
	 * Get a new connection using JDBC driver
	 * @return Connection object
	 */
	public Connection getConnection() throws DbException {
		try {
			if (con != null && !con.isClosed()) {
				return con;
			}
			closeConnection();
			con = DriverManager.getConnection(jdbcUrl, username, password);
		} catch (SQLException e) {
			throw new DbException("Error creating JDBC connection", e);
		}
		return con;
	}

	/**
	 * Quietly close a connection
	 */
	public void closeConnection() {
		JdbcUtil.closeQuietly(con);
		con = null;
	}

	public static String toSqlStr(String s) {
		return "'" + s + "'";
	}

	/**
	 * Reads rows from a file, read from the client host, into a table using JDBC connection
	 * @param filename File to read from
	 * @param tableName Table to read into
	 * @return Row count for SQL DML statements
	 */
	public int importData(String filename, String tableName) throws DbException {
		Statement stmt = null;
		int result = -1;
		try {
			Connection con = getConnection();
			stmt = con.createStatement();
			String sql = "LOAD DATA LOCAL INFILE " + toSqlStr(filename) //
					+ " INTO TABLE " + tableName //
					+ " FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\\\'" //
					+ " LINES TERMINATED BY '\\n' " //
					+ " IGNORE 1 ROWS";//
			result = stmt.executeUpdate(sql);
			con.commit();
		} catch (SQLException e) {
			throw new DbException("Error importing data file", e);
		} finally {
			JdbcUtil.closeQuietly(stmt);
		}
		return result;
	}

	/**
	 * Writes rows, resulting from a query, to a file using a JDBC Connection
	 * @param filename File on which to write rows
	 * @param query Query to fetch rows
	 */
	public void exportData(String filename, String query) throws DbException {
		Statement stmt = null;
		try {
			stmt = getConnection().createStatement();
			// For comma separated file
			String sql = query + //
					"INTO OUTFILE " + toSqlStr(filename) //
					+ " FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\\\'" //
					+ " LINES TERMINATED BY '\\n' " //
					+ " IGNORE 1 ROWS";//
			stmt.executeQuery(sql);
		} catch (SQLException e) {
			throw new DbException("Error exporting data file", e);
		} finally {
			JdbcUtil.closeQuietly(stmt);
		}
	}

	/**
	 * Execute an SQL query and return the output as a ResultRow object list
	 * @param query Query to be executed
	 * @return Result of query execution as a list of ResultRow objects
	 * @throws DbException
	 */
	public List<ResultRow> executeQuery(String query) throws DbException {
		Statement stmt = null;
		List<ResultRow> rowList = null;
		try {
			stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
			HashMap<String, Integer> columnMap = JdbcUtil.getColumMap(rs);
			rowList = new ArrayList<ResultRow>();
			while (rs.next()) {
				rowList.add(JdbcUtil.getResultRow(rs, columnMap));
			}
		} catch (SQLException e) {
			throw new DbException("Error running DB query", e);
		} finally {
			JdbcUtil.closeQuietly(stmt);
		}
		return rowList;
	}

	public boolean execute(String query) throws DbException {
		Statement stmt = null;
		try {
			stmt = getConnection().createStatement();
			return stmt.execute(query);
		} catch (SQLException e) {
			throw new DbException("Error running DB query", e);
		} finally {
			JdbcUtil.closeQuietly(stmt);
		}
	}

}
