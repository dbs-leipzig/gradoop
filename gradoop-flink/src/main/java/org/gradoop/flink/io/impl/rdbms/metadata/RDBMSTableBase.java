package org.gradoop.flink.io.impl.rdbms.metadata;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.LinkedHashMap;

abstract class RDBMSTableBase {

	// rdbms' table name
	private String tableName;

	// storing all primary key names
	private ArrayList<String> primaryKeys;

	// storing all foreign key names and belonging referenced table names
	private LinkedHashMap<String, String> foreignKeys;

	// storing all attribute names and belonging jdbc data type (primary key and
	// foreign key attributes included)
	private LinkedHashMap<String, JDBCType> attributes;

	// direction indicator needed to distinguish 1:1/1:n and n:m relations
	private Boolean directionIndicator;

	private int numberOfRows;
	private RowHeader rowHeader;
	private ArrayList<JDBCType> jdbcTypes;

	/**
	 * constructor
	 * 
	 * @param tableName
	 *            name of rdbms table
	 * @param primaryKeys
	 *            set of primary keys 
	 * @param foreignKeys
	 *            map of pairs, consists of foreign key name and belonging referenced table name 
	 * @param attributes
	 *            map of pairs, consists of attribute name and belonging jdbc data type (primary
	 *            and foreign keys included)
	 * @param directionIndicator
	 *            true if 1:1,1:n relation,
	 *            false if n:m relation
	 * @param numberOfRows
	 *            number of rows of the rdbms table
	 * @param jdbcTypes
	 *            jdbcTypes of attribute
	 */
	public RDBMSTableBase(String tableName, ArrayList<String> primaryKeys, LinkedHashMap<String, String> foreignKeys,
			LinkedHashMap<String, JDBCType> attributes, Boolean directionIndicator, int numberOfRows,
			ArrayList<JDBCType> jdbcTypes) {
		this.tableName = tableName;
		this.primaryKeys = primaryKeys;
		this.foreignKeys = foreignKeys;
		this.attributes = attributes;
		this.directionIndicator = directionIndicator;
		this.numberOfRows = numberOfRows;
		this.jdbcTypes = jdbcTypes;
	}

	/**
	 * basic constructor
	 */
	public RDBMSTableBase() {
		this.primaryKeys = new ArrayList<String>();
		this.foreignKeys = new LinkedHashMap<String, String>();
		this.attributes = new LinkedHashMap<String, JDBCType>();
		this.jdbcTypes = new ArrayList<JDBCType>();
		this.rowHeader = new RowHeader();
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

	public Boolean getDirectionIndicator() {
		return directionIndicator;
	}

	public void setDirectionIndicator(Boolean directionIndicator) {
		this.directionIndicator = directionIndicator;
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
