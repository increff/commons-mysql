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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import com.nextscm.commons.lang.CmdUtil;
import com.nextscm.commons.lang.FileUtil;
import com.nextscm.commons.lang.IoUtil;

/**
 * Provides access to certain DB functions that may be run directly such as to create a database, pull data, etc
 * WARNING: This class uses TSV format for files. It automatically applies
 * <b>.tsv</b> to the given file-names
 */
public class DbCmd {

	private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	private String localDir;
	private SqlUtil sqlUtil;

	private static final String DBCMD = "dbcmd";

	/**
	 * Instantiate a DbCmd object to interact with the database
	 * @param localDir Directory to store files locally for reading/writing
	 * @param host Host information of database
	 * @param username Database username
	 * @param password Database password
	 * @param schema Database schema name
	 */
	public DbCmd(String localDir, String host, String username, String password, String schema) {
		this.localDir = localDir;
		this.sqlUtil = new SqlUtil(host, username, password, schema);

	}

	/**
	 * Runs admin SQL Command: CREATE SCHEMA `dummy` DEFAULT CHARACTER SET utf8
	 * @param schema Name for schema
	 */
	public void createDb(String schema) throws DbException {
		String query = "CREATE DATABASE " + schema + " DEFAULT CHARACTER SET utf8";
		String[] cmd = sqlUtil.getAdminCmd(query);
		Redirect redirect = getLogRedirect();

		try {
			CmdUtil.runCmd(cmd, redirect, redirect);
		} catch (IOException | InterruptedException e) {
			throw new DbException("Error creating schema: " + schema, e);
		}
	}

	/**
	 * Runs SQL query to drop a database (if it exists)
	 * @param schema Name of schema to drop
	 */
	public void dropDb(String schema) throws DbException {
		String query = "DROP DATABASE IF EXISTS " + schema;
		String[] cmd = sqlUtil.getAdminCmd(query);
		Redirect redirect = getLogRedirect();

		try {
			CmdUtil.runCmd(cmd, redirect, redirect);
		} catch (IOException | InterruptedException e) {
			throw new DbException("Error dropping schema: " + schema, e);
		}

	}

	/**
	 * Pull a collection of files, represented by the fileQueryMap. fileQueryMap maps the location to where the file has
	 * to be pulled to the query that has to be run for pulling the corresponding file
	 * @param fileQueryMap Mapping of destination file and pull query
	 */
	public void pull(HashMap<String, String> fileQueryMap) throws DbException {
		ArrayList<String> failedFiles = new ArrayList<>();
		for (String fileName : fileQueryMap.keySet()) {
			try {
				pull(fileName, fileQueryMap.get(fileName));
			} catch (DbException e) {
				failedFiles.add(fileName);
			}
		}
		if (failedFiles.size() > 0) {
			throw new DbException("Error pulling files: " + String.join(",", failedFiles));
		}
	}

	/**
	 * Construct a full (non admin) SQL query and then execute it
	 * @param fullQuery SQL query to be run
	 */
	public void processQuery(String fullQuery) throws DbException {
		String[] cmd = sqlUtil.getQueryCmd(fullQuery);

		Redirect redirectOut = null;
		Redirect redirectError = getLogRedirect();

		try {
			CmdUtil.runCmd(cmd, redirectOut, redirectError);
		} catch (IOException | InterruptedException e) {
			throw new DbException("Error processing  query");
		}
	}

	/**
	 * Pull data from database and append to the specified TSV file
	 * @param fileName Target file for appending
	 * @param fullQuery SQL query for pulling data
	 */
	public void pullAppend(String fileName, String fullQuery) throws DbException {
		fileName = fileName + ".tsv";
		String outFilePath = getFilePath(fileName);
		String[] cmd = sqlUtil.getQueryCmd(fullQuery);

		Redirect redirectOut = Redirect.appendTo(new File(outFilePath));
		Redirect redirectError = getLogRedirect();

		try {
			CmdUtil.runCmd(cmd, redirectOut, redirectError);
		} catch (IOException | InterruptedException e) {
			throw new DbException("Error pulling file: " + fileName, e);
		}
	}

	/**
	 * Pull a file from the DB and store in specified location
	 * @param fileName Location to store pulled file
	 * @param fullQuery Full query to be executed for pulling
	 */
	public void pull(String fileName, String fullQuery) throws DbException {
		String outFilePath = getFilePath(fileName);
		String[] cmd = sqlUtil.getQueryCmd(fullQuery);

		FileUtil.deleteFile(outFilePath);
		Redirect redirectOut = Redirect.to(new File(outFilePath));
		Redirect redirectError = getLogRedirect();

		try {
			CmdUtil.runCmd(cmd, redirectOut, redirectError);
		} catch (IOException | InterruptedException e) {
			throw new DbException("Error pulling file: " + fileName, e);
		}
	}

	// DELETE FILE IF EXISTS

	// PUSH TO DATABASE

	/**
	 * Push a set of files to the database
	 * @param fileNames Set of names of files to be pushed
	 */
	public void push(Set<String> fileNames) throws DbException {
		ArrayList<String> failedFiles = new ArrayList<>();
		for (String fileName : fileNames) {
			try {
				push(fileName);
			} catch (DbException e) {
				failedFiles.add(fileName);
			}
		}
		if (failedFiles.size() > 0) {
			throw new DbException("Error pushing files: " + String.join(",", failedFiles));
		}
	}

	/**
	 * Push contents of a single file on to the database based on format specified in getImportCmd()
	 * @param fileName Name of file to be pushed
	 */
	public void push(String fileName) throws DbException {
		String filePath = getFilePath(fileName);
		String columns = getColumns(filePath);
		String[] cmd = sqlUtil.getImportCmd(filePath, columns);

		Redirect redirect = getLogRedirect();
		try {
			CmdUtil.runCmd(cmd, redirect, redirect);
		} catch (IOException | InterruptedException e) {
			throw new DbException("Error pushing to table: " + fileName, e);
		}
	}

	/**
	 * Delete all records in a table using TRUNCATE
	 * @param tableName
	 * @throws DbException
	 */
	public void truncate(String tableName) throws DbException {
		Redirect redirectAll = getLogRedirect();
		String[] cmd = sqlUtil.getTruncateCmd(tableName);
		try {
			CmdUtil.runCmd(cmd, redirectAll, redirectAll);
		} catch (IOException | InterruptedException e) {
			throw new DbException("Error truncating table: " + tableName, e);
		}
	}

	/**
	 * Delete all records in a table using DELETE
	 * @param tableName Name of table to be deleted
	 */
		public void delete(String tableName) throws DbException {
		Redirect redirectAll = getLogRedirect();
		String[] cmd = sqlUtil.getDeleteAllCommand(tableName);
		try {
			CmdUtil.runCmd(cmd, redirectAll, redirectAll);
		} catch (IOException | InterruptedException e) {
			throw new DbException("Error deleting from table table: " + tableName, e);
		}
	}

	// UTILITY METHODS

	/**
	 * Prepend local directory path to specified input
	 * @param fileName Path to which prepend the local directory path
	 * @return Concatenated path
	 */
	private String getFilePath(String fileName) {
		return localDir + File.separator + fileName;
	}

	/**
	 * Set redirect to log file
	 * @return Redirect object
	 */
	private Redirect getLogRedirect() {
		Redirect redirectAll = Redirect.appendTo(getLogFile());
		return redirectAll;
	}

	/**
	 * @return Log file
	 */
	private File getLogFile() {
		Date d = new Date();
		String suffix = df.format(d);
		String logFilePath = getFilePath(DBCMD) + "-" + suffix + ".log";
		File logFile = new File(logFilePath);
		return logFile;
	}

	/**
	 * Names of all columns read from a TSV file
	 * @param filePath Path to TSV
	 * @return Column names as a single, comma separated string
	 */
	public static String getColumns(String filePath) throws DbException {
		String columns = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filePath));
			columns = br.readLine();
		} catch (IOException e) {
			throw new DbException("Error reading header for file: " + filePath, e);
		} finally {
			IoUtil.closeQuietly(br);
		}
		return columns.replaceAll("\t", ",");
	}

}
