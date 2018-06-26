package org.gradoop.flink.io.impl.rdbms.metadata;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

public class NodeTable {

	private String tableName;

	// storing all primary key names
	private ArrayList<String> primaryKeys;

	// storing all foreign key names and belonging referenced table names
	private LinkedHashMap<String, String> foreignKeys;

	private RowHeader rowHeader;
	
	

	public NodeTable() {
		primaryKeys = new ArrayList<String>();
		foreignKeys = new LinkedHashMap<String,String>();
		rowHeader = new RowHeader();
	}

	
	/**
	 * computes sql query, row header and belonging jdbctypes for node conversion
	 * 
	 * @return
	 */
	public String getNodeSqlQuery() {
		this.rowHeader = new RowHeader();

		String sql = "SELECT ";

		int i = 0;
		for (String pk : primaryKeys) {
			sql = sql + pk + ",";
			RowHeaderTuple rht = new RowHeaderTuple(pk, "pk", i);
			rowHeader.getRowHeader().add(rht);
			i++;
		}
		for (Entry<String, String> fk : foreignKeys.entrySet()) {
			sql += fk.getKey() + ",";
			RowHeaderTuple rht = new RowHeaderTuple(fk.getKey(), "fk", i);
			rowHeader.getRowHeader().add(rht);
			i++;
		}
		for (String att : getSimpleAttributes()) {
			sql += att + ",";
			RowHeaderTuple rht = new RowHeaderTuple(att, "att", i);
			rowHeader.getRowHeader().add(rht);
			i++;
		}
		return sql = sql.substring(0, sql.length() - 1) + " FROM " + tableName;
	}

	/**
	 * collects all non primary key and non foreign key attributes
	 */
	public ArrayList<String> getSimpleAttributes() {
		ArrayList<String> simpleAttributes = new ArrayList<String>();
		HashSet<String> fkAttributes = new HashSet<String>();
		HashSet<String> pkAttributes = new HashSet<String>();

		for (String pk : primaryKeys) {
			pkAttributes.add(pk);
		}
		for (Entry<String, String> fk : foreignKeys.entrySet()) {
			fkAttributes.add(fk.getKey());
		}
		for (Entry<String, JDBCType> attr : attributes.entrySet()) {
			if (!fkAttributes.contains(attr.getKey()) && !pkAttributes.contains(attr.getKey())) {
				simpleAttributes.add(attr.getKey());
			}
		}
		return simpleAttributes;
	}

	// ******************
	// getter and setter
	// ******************

	public ArrayList<JDBCType> getjdbcTypes() {
		return jdbcTypes;
	}

	public void setjdbcTypes(ArrayList<JDBCType> jdbcTypes) {
		this.jdbcTypes = jdbcTypes;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public ArrayList<String> getPrimaryKey() {
		return primaryKeys;
	}

	public void setPrimaryKey(ArrayList<String> primaryKey) {
		this.primaryKeys = primaryKey;
	}

	public LinkedHashMap<String, String> getForeignKeys() {
		return foreignKeys;
	}

	public void setForeignKeys(LinkedHashMap<String, String> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}

	public LinkedHashMap<String, JDBCType> getAttributes() {
		return attributes;
	}

	public void setAttributes(LinkedHashMap<String, JDBCType> attributes) {
		this.attributes = attributes;
	}

	public int getNumberOfRows() {
		return numberOfRows;
	}

	public void setNumberOfRows(int numberOfRows) {
		this.numberOfRows = numberOfRows;
	}

	public RowHeader getRowHeader() {
		return rowHeader;
	}

	public void setRowHeader(RowHeader rowHeader) {
		this.rowHeader = rowHeader;
	}

	/**
	 * returns clone of rdbms table
	 */
	public RDBMSTable clone() {
		return new RDBMSTable(tableName, (ArrayList<String>) primaryKeys.clone(),
				(LinkedHashMap<String, String>) foreignKeys.clone(),
				(LinkedHashMap<String, JDBCType>) attributes.clone(), directionIndicator, numberOfRows, jdbcTypes);
	}
}
