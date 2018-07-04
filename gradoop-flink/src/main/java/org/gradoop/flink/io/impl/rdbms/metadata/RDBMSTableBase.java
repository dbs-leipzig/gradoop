package org.gradoop.flink.io.impl.rdbms.metadata;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.io.impl.rdbms.tuples.AttTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.FKTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.PKTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.TableTuple;

public class RDBMSTableBase {
	private String tableName;
	private ArrayList<NameTypeTuple> primaryKeys;
	private ArrayList<Tuple2<NameTypeTuple,String>> foreignKeys;
	private ArrayList<NameTypeTuple> furtherAttributes;
	private int rowCount;

	public RDBMSTableBase(String tableName, ArrayList<NameTypeTuple> primaryKeys,
			ArrayList<Tuple2<NameTypeTuple,String>> foreignKeys, ArrayList<NameTypeTuple> furtherAttributes, int rowCount) {
		this.tableName = tableName;
		this.primaryKeys = primaryKeys;
		this.foreignKeys = foreignKeys;
		this.furtherAttributes = furtherAttributes;
		this.rowCount = rowCount;
	}

	/*
	 * Getter and Setter
	 */
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

	public ArrayList<Tuple2<NameTypeTuple,String>> getForeignKeys() {
		return foreignKeys;
	}

	public void setForeignKeys(ArrayList<Tuple2<NameTypeTuple,String>> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}

	public ArrayList<NameTypeTuple> getFurtherAttributes() {
		return furtherAttributes;
	}

	public void setFurtherAttributes(ArrayList<NameTypeTuple> furtherAttributes) {
		this.furtherAttributes = furtherAttributes;
	}

	public int getRowCount() {
		return rowCount;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}
}
