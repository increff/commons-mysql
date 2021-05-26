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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.Date;
import java.util.HashMap;

public class ResultRow {

	private String[] tokens;
	private HashMap<String, Integer> columns;
	int row;

	public ResultRow(HashMap<String, Integer> index) {
		this.columns = index;
		this.tokens = null;
	}

	/* LOW LEVEL SETTER AND GETTER */

	protected int getIndex(String col) {
		Integer i = columns.get(col);
		if (i == null) {
			throw new RuntimeException("Invalid column:" + col);
		}
		return i;
	}

	public String[] getTokens() {
		return tokens;
	}

	public String getValue(int index) {
		return tokens[index];
	}

	public String getValue(String col) {
		return getValue(getIndex(col));
	}

	public void setTokens(String[] tokens) {
		this.tokens = tokens;
	}

	public void setTokens(Object[] tokens) {
		for (int i = 0; i < tokens.length; i++) {
			setToken(i, tokens[i]);
		}
	}

	public void setToken(int index, Object token) {
		tokens[index] = token == null ? null : token.toString();
	}

	public void setToken(String col, Object token) {
		setToken(getIndex(col), token);
	}

	// META INFORMATION
	public void setRow(int i) {
		this.row = i;
	}

	public int getRow() {
		return row;
	}

	public HashMap<String, Integer> getColumns() {
		return columns;
	}

	/* Helper methods */

	// STRING BASE GETS
	public String getString(String col) {
		return getValue(col);
	}

	public Date getDate(String col, DateFormat df) throws ParseException {
		String s = getValue(col);
		return s == null ? null : df.parse(s);
	}

	public Long getLong(String col) {
		String s = getValue(col);
		return s == null ? null : Long.parseLong(s);
	}

	public Double getDouble(String col) {
		String s = getValue(col);
		return s == null ? null : Double.parseDouble(s);
	}

	public BigInteger getBigInt(String col) {
		String s = getValue(col);
		return s == null ? null : new BigInteger(s);
	}

	public BigDecimal getBigDecimal(String col) {
		String s = getValue(col);
		return s == null ? null : new BigDecimal(s);
	}

	public Integer getInteger(String col) {
		String s = getValue(col);
		return s == null ? null : Integer.parseInt(s);
	}

	public Boolean getBoolean(String col) {
		String s = getValue(col);
		return s == null ? null : Boolean.parseBoolean(s);
	}

	public Boolean getBooleanFromInt(String col) {
		String s = getValue(col);
		return s == null ? null : s.equals("1");
	}

	public LocalDate getLocalDate(String col) {
		String s = getValue(col);
		return s == null ? null : LocalDate.parse(s);
	}

	public YearMonth getYearMonth(String col) {
		String s = getValue(col);
		return s == null ? null : YearMonth.parse(s);
	}

	/* INTEGER BASED GETS */
	public String getStringIntern(int col) {
		String s = getValue(col);
		return s == null ? null : s.intern();
	}

	public String getString(int col) {
		return getValue(col);
	}

	public Long getLong(int col) {
		String s = getValue(col);
		return s == null ? null : Long.parseLong(s);
	}

	public Double getDouble(int col) {
		String s = getValue(col);
		return s == null ? null : Double.parseDouble(s);
	}

	public BigInteger getBigInteger(int col) {
		String s = getValue(col);
		return s == null ? null : new BigInteger(s);
	}

	public BigDecimal getBigDecimal(int col) {
		String s = getValue(col);
		return s == null ? null : new BigDecimal(s);
	}

	public Integer getInteger(int col) {
		String s = getValue(col);
		return s == null ? null : Integer.parseInt(s);
	}

	public Boolean getBoolean(int col) {
		String s = getValue(col);
		return s == null ? null : Boolean.parseBoolean(s);
	}

	public LocalDate getLocalDate(int col) {
		String s = getValue(col);
		return s == null ? null : LocalDate.parse(s);
	}

	public YearMonth getYearMonth(int col) {
		String s = getValue(col);
		return s == null ? null : YearMonth.parse(s);
	}

}
