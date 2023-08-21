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

public class SqlUtil {

	private String host;
	private String username;
	private String password;
	private String schema;

	public SqlUtil(String host, String username, String password, String schema) {
		this.host = host;
		this.username = username;
		this.password = password;
		this.schema = schema;
	}

	public static String[] tokenize(String s) {
		return s.split(";");
	}

	public static String escape(String str) {
		OperatingSystem os = OperatingSystem.getOs();
		if (os.equals(OperatingSystem.windows)) {
			return "\"" + str + "\"";
		}
		return str;
	}

	// Commands

	/**
	 * Convert a provided SQL query into a fully composed Query Command array with required SQL options
	 * @param query SQL Query to be converted to complete command
	 * @return String array representing complete command
	 */
	public String[] getQueryCmd(String query) {
		String[] cmd = new String[] { "mysql", "--quick", "--connect-timeout=5", "--host=" + host, "--user=" + username,
				"--password=" + password, schema, "-e", SqlUtil.escape(query) };
		return cmd;
	}


	public String[] getAdminCmd(String query) {
		String[] cmd = new String[] { "mysql", "--host=" + host, "--user=" + username, "--password=" + password, "-e",
				query };
		return cmd;
	}

	public String[] getImportCmd(String filePath, String columns) {
		String lineSeparator = "\n";
		String fieldSeparator = "\t";

		if (OperatingSystem.getOs() == OperatingSystem.windows) {
			filePath = "\"" + filePath + "\"";
			lineSeparator = "\r\n";
		}
		String[] s = {"mysql", "--host="+host,"--user="+username, "--password="+password, "-e",
			"LOAD DATA LOCAL INFILE  '" +filePath+ "'  INTO TABLE `"+schema+"`."+getTableName(filePath)+"  FIELDS TERMINATED BY '"+fieldSeparator+"' LINES TERMINATED BY '"+lineSeparator+"' IGNORE 1 LINES ("+columns+")"};
		return s;
	}

	/**
	 * Get SQL command for deleting records in the table (while maintaining table integrity)
	 * @param tableName Target table name
	 * @return String array representing delete all command
	 */
	public String[] getDeleteAllCommand(String tableName) {
		String query = "delete from " + tableName;
		return getQueryCmd(query);
	}

	/**
	 * Get SQL command for truncating/removing all records from a table
	 * @param tableName Target table name
	 * @return String array representing truncate table command
	 */
	public String[] getTruncateCmd(String tableName) {
		String query = "truncate table " + tableName;
		return getQueryCmd(query);
	}
	private String getTableName(String filePath) {
		int slashIndex = filePath.lastIndexOf('/');
		int dotIndex = filePath.lastIndexOf('.');
		return filePath.substring(slashIndex + 1, dotIndex);
	}
}
