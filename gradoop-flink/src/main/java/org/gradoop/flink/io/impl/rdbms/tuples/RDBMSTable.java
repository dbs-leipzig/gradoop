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

/**
 * basic representation of an rdbms table storing important metadata informations
 * @author pc
 *
 */
public class RDBMSTable implements Serializable, Cloneable {

	private String tableName;
	private LinkedHashSet<String> primaryKeys;
	private LinkedHashMap<String, String> foreignKeys;
	private LinkedHashMap<String, JDBCType> attributes;
	private Boolean directionIndicator;
	private int numberOfRows;
	private RowHeader rowHeader;
	private ArrayList<JDBCType> jdbcTypes;

	/**
	 * constructor
	 * @param tableName name of the rdbms table	
	 * @param primaryKeys set of primary keys of rdbms table
	 * @param foreignKeys set of foreign keys of rdbms table
	 * @param attributes set of attributes of rdbms table (primary/ foreign key included)
	 * @param directionIndicator true if 1:1,1:n relation, false if n:m relation
	 * @param numberOfRows number of rows of the rdbms table
	 * @param jdbcTypes jdbcTypes of attribute values of rdbms table
	 */
	public RDBMSTable(String tableName, LinkedHashSet<String> primaryKeys, LinkedHashMap<String, String> foreignKeys,
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
	public RDBMSTable() {
		this.primaryKeys = new LinkedHashSet<String>();
		this.foreignKeys = new LinkedHashMap<String, String>();
		this.attributes = new LinkedHashMap<String, JDBCType>();
		this.jdbcTypes = new ArrayList<JDBCType>();
	}

	/**
	 * computes sql query, row header and belonging jdbctypes for node conversion
	 * @return
	 */
	public String getNodeSqlQuery() {
		this.jdbcTypes.clear();
		this.rowHeader = new RowHeader();
		
		String sql = "SELECT ";
		
		int i = 0;
		for (String pk : primaryKeys) {
			sql = sql + pk + ",";
			RowHeaderTuple rht = new RowHeaderTuple(pk, "pk", i);
			rowHeader.getRowHeader().add(rht);
			jdbcTypes.add(attributes.get(pk));
			i++;
		}
		for (Entry<String, String> fk : foreignKeys.entrySet()) {
			sql += fk.getKey() + ",";
			RowHeaderTuple rht = new RowHeaderTuple(fk.getKey(), "fk", i);
			rowHeader.getRowHeader().add(rht);
			jdbcTypes.add(attributes.get(fk.getKey()));
			i++;
		}
		for (String att : getSimpleAttributes()) {
			sql += att + ",";
			RowHeaderTuple rht = new RowHeaderTuple(att, "att", i);
			rowHeader.getRowHeader().add(rht);
			jdbcTypes.add(attributes.get(att));
			i++;
		}
		return sql = sql.substring(0, sql.length() - 1) + " FROM " + tableName;
	}

	/**
	 * computes sql query, row header and belonging jdbctypes for edge conversion
	 * @return
	 */
	public String getEdgeSqlQuery() {
		this.jdbcTypes.clear();
		this.rowHeader = new RowHeader();
	
		String sql = "SELECT ";
		
		int i = 0;
		
		/*
		 * creates sql query for m:n relations
		 */
		if (foreignKeys.size() == 2) {
			for (Entry<String, String> fk : foreignKeys.entrySet()) {
				sql += fk.getKey() + ",";
				RowHeaderTuple rht = new RowHeaderTuple(fk.getKey(), "fk", i);
				rowHeader.getRowHeader().add(rht);
				jdbcTypes.add(attributes.get(fk.getKey()));
				i++;
			}
			for (String att : getSimpleAttributes()) {
				sql += att + ",";
				RowHeaderTuple rht = new RowHeaderTuple(att, "att", i);
				rowHeader.getRowHeader().add(rht);
				jdbcTypes.add(attributes.get(att));
				i++;
			}
		} else 
		
		/*
		 * creates sql query for 1:1, 1:n relations
		 */
		{
			for (String pk : primaryKeys) {
				sql += pk + ",";
				RowHeaderTuple rht = new RowHeaderTuple(pk, "pk", i);
				rowHeader.getRowHeader().add(rht);
				jdbcTypes.add(attributes.get(pk));
				i++;
			}
			for (Entry<String, String> fk : foreignKeys.entrySet()) {
				sql += fk.getKey() + ",";
				RowHeaderTuple rht = new RowHeaderTuple(fk.getKey(), "fk", i);
				rowHeader.getRowHeader().add(rht);
				jdbcTypes.add(attributes.get(fk.getKey()));
				i++;
			}
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

	//******************
	// getter and setter
	//******************
	
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

	public LinkedHashSet<String> getPrimaryKey() {
		return primaryKeys;
	}

	public void setPrimaryKey(LinkedHashSet<String> primaryKey) {
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
	 * return clone of rdbms table
	 */
	public RDBMSTable clone() {
		return new RDBMSTable(tableName, (LinkedHashSet<String>) primaryKeys.clone(),
				(LinkedHashMap<String, String>) foreignKeys.clone(),
				(LinkedHashMap<String, JDBCType>) attributes.clone(), directionIndicator, numberOfRows, jdbcTypes);
	}
}
