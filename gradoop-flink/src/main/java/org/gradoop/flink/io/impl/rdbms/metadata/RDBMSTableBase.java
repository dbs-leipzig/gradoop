package org.gradoop.flink.io.impl.rdbms.metadata;

import java.util.ArrayList;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.io.impl.rdbms.tuples.FkTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTypeTuple;

/**
 * Represents the relational database schema
 */
public class RDBMSTableBase {
	
	/**
	 * Name of database table
	 */
	private String tableName;
	
	/**
	 * List of primary keys of database table
	 */
	private ArrayList<NameTypeTuple> primaryKeys;
	
	/**
	 * List of foreign key of database table
	 */
	private ArrayList<FkTuple> foreignKeys;
	
	/**
	 * List of further attributes (no primary, foreign key attributes) of database table
	 */
	private ArrayList<NameTypeTypeTuple> furtherAttributes;
	
	/**
	 * Number of rows of table
	 */
	private int rowCount;

	/**
	 * Constructor
	 * @param tableName Name of database table
	 * @param primaryKeys List of primary keys
	 * @param foreignKeys List of foreign keys
	 * @param furtherAttributes List of further attributes
	 * @param rowCount Number of rows
	 */
	public RDBMSTableBase(String tableName, ArrayList<NameTypeTuple> primaryKeys,
			ArrayList<FkTuple> foreignKeys, ArrayList<NameTypeTypeTuple> furtherAttributes, int rowCount) {
		this.tableName = tableName;
		this.primaryKeys = primaryKeys;
		this.foreignKeys = foreignKeys;
		this.furtherAttributes = furtherAttributes;
		this.rowCount = rowCount;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public ArrayList<NameTypeTuple> getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(ArrayList<NameTypeTuple> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public ArrayList<FkTuple> getForeignKeys() {
		return foreignKeys;
	}

	public void setForeignKeys(ArrayList<FkTuple> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}

	public ArrayList<NameTypeTypeTuple> getFurtherAttributes() {
		return furtherAttributes;
	}

	public void setFurtherAttributes(ArrayList<NameTypeTypeTuple> furtherAttributes) {
		this.furtherAttributes = furtherAttributes;
	}

	public int getRowCount() {
		return rowCount;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}
}
