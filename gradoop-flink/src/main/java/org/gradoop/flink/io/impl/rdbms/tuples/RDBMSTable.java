package org.gradoop.flink.io.impl.rdbms.tuples;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.java.tuple.Tuple4;

public class RDBMSTable {

	public String tableName;
	public String primaryKey;
	public HashMap<String,String> foreignKeys;

	public RDBMSTable(String tableName, String primaryKey, HashMap<String,String> foreignKeys) {
		this.tableName = tableName;
		this.primaryKey = primaryKey;
		this.foreignKeys = foreignKeys;
	}
	
	public RDBMSTable(){
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public HashMap<String, String> getForeignKeys() {
		return foreignKeys;
	}

	public void setForeignKeys(HashMap<String, String> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}
}
