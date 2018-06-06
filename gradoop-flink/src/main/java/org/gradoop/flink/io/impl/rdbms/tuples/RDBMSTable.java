package org.gradoop.flink.io.impl.rdbms.tuples;

import java.io.Serializable;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map.Entry;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

public class RDBMSTable implements Serializable, Cloneable {

	private String tableName;
	private LinkedHashSet<String> primaryKey;
	private LinkedHashMap<String, String> foreignKeys;
	private LinkedHashMap<String, JDBCType> attributes;
	private Boolean directionIndicator;
	private int numberOfRows;
	private RowHeader rowHeader;
	private ArrayList<JDBCType> sqlTypes;

	public RDBMSTable(String tableName, LinkedHashSet<String> primaryKey, LinkedHashMap<String, String> foreignKeys,
			LinkedHashMap<String, JDBCType> attributes, Boolean directionIndicator, int numberOfRows,
			ArrayList<JDBCType> sqlTypes) {
		this.tableName = tableName;
		this.primaryKey = primaryKey;
		this.foreignKeys = foreignKeys;
		this.attributes = attributes;
		this.directionIndicator = directionIndicator;
		this.numberOfRows = numberOfRows;
		this.sqlTypes = sqlTypes;
	}

	public RDBMSTable() {
		this.primaryKey = new LinkedHashSet<String>();
		this.foreignKeys = new LinkedHashMap<String, String>();
		this.attributes = new LinkedHashMap<String, JDBCType>();
		this.sqlTypes = new ArrayList<JDBCType>();
	}

	public ArrayList<JDBCType> getSqlTypes() {
		return sqlTypes;
	}

	public void setSqlTypes(ArrayList<JDBCType> sqlTypes) {
		this.sqlTypes = sqlTypes;
	}

	public String getNodeSqlQuery() {
		this.sqlTypes.clear();
		this.rowHeader = new RowHeader();
		String sql = "SELECT ";
		int i = 0;
		for (String pk : primaryKey) {
			sql = sql + pk + ",";
			RowHeaderTuple rht = new RowHeaderTuple(pk, "pk", i);
			rowHeader.getRowHeader().add(rht);
			sqlTypes.add(attributes.get(pk));
			i++;
		}
		for (String att : getSimpleAttributes()) {
			sql += att + ",";
			RowHeaderTuple rht = new RowHeaderTuple(att, "att", i);
			rowHeader.getRowHeader().add(rht);
			sqlTypes.add(attributes.get(att));
			i++;
		}
		return sql = sql.substring(0, sql.length() - 1) + " FROM " + tableName;
	}

	public String getEdgeSqlQuery() {
		this.sqlTypes.clear();
		this.rowHeader = new RowHeader();
		String sql = "SELECT ";
		int i = 0;
		if (foreignKeys.size() == 2) {
			for (Entry<String, String> fk : foreignKeys.entrySet()) {
				sql += fk.getKey() + ",";
				RowHeaderTuple rht = new RowHeaderTuple(fk.getKey(), "fk", i);
				rowHeader.getRowHeader().add(rht);
				sqlTypes.add(attributes.get(fk.getKey()));
				i++;
			}
			for (String att : getSimpleAttributes()) {
				sql += att + ",";
				RowHeaderTuple rht = new RowHeaderTuple(att, "att", i);
				rowHeader.getRowHeader().add(rht);
				sqlTypes.add(attributes.get(att));
				i++;
			}
		} else {
			for (String pk : primaryKey) {
				sql += pk + ",";
				RowHeaderTuple rht = new RowHeaderTuple(pk, "pk", i);
				rowHeader.getRowHeader().add(rht);
				sqlTypes.add(attributes.get(pk));
				i++;
			}
			for (Entry<String, String> fk : foreignKeys.entrySet()) {
				sql += fk.getKey() + ",";
				RowHeaderTuple rht = new RowHeaderTuple(fk.getKey(), "fk", i);
				rowHeader.getRowHeader().add(rht);
				sqlTypes.add(attributes.get(fk.getKey()));
				i++;
			}
		}
		return sql = sql.substring(0, sql.length() - 1) + " FROM " + tableName;
	}

	public ArrayList<String> getSimpleAttributes() {
		ArrayList<String> simpleAttributes = new ArrayList<String>();
		HashSet<String> fkAttributes = new HashSet<String>();
		HashSet<String> pkAttributes = new HashSet<String>();

		for (String pk : primaryKey) {
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

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public LinkedHashSet<String> getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(LinkedHashSet<String> primaryKey) {
		this.primaryKey = primaryKey;
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

	public RDBMSTable clone() {
		return new RDBMSTable(tableName, (LinkedHashSet<String>) primaryKey.clone(),
				(LinkedHashMap<String, String>) foreignKeys.clone(),
				(LinkedHashMap<String, JDBCType>) attributes.clone(), directionIndicator, numberOfRows, sqlTypes);
	}
}
