package org.gradoop.flink.io.impl.rdbms.metadata;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.gradoop.flink.io.impl.rdbms.tuples.AttTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.FKTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.PKTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.TableTuple;

public class RDBMSTableBase {
	private String tableName;
	private ArrayList<PKTuple> primaryKeys;
	private ArrayList<FKTuple> foreignKeys;
	private ArrayList<AttTuple> furtherAttributes;
	private int rowCount;

	public RDBMSTableBase(String tableName, ArrayList<PKTuple> primaryKeys,
			ArrayList<FKTuple> foreignKeys, ArrayList<AttTuple> furtherAttributes, int rowCount) {
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

	public ArrayList<PKTuple> getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(ArrayList<PKTuple> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public ArrayList<FKTuple> getForeignKeys() {
		return foreignKeys;
	}

	public void setForeignKeys(ArrayList<FKTuple> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}

	public ArrayList<AttTuple> getFurtherAttributes() {
		return furtherAttributes;
	}

	public void setFurtherAttributes(ArrayList<AttTuple> furtherAttributes) {
		this.furtherAttributes = furtherAttributes;
	}

	public int getRowCount() {
		return rowCount;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}

//	public ArrayList<JDBCType> getTypes() {
//		ArrayList<JDBCType> types = new ArrayList<JDBCType>();
//		for (TableTuple pks : primaryKeys) {
//			types.add(pks.f3);
//		}
//		if (!foreignKeys.isEmpty()) {
//			for (Entry<TableTuple, String> fks : foreignKeys.entrySet()) {
//				types.add(fks.getKey().f3);
//			}
//		}
//		if (!furtherAttributes.isEmpty()) {
//			for (TableTuple att : furtherAttributes) {
//				types.add(att.f3);
//			}
//		}
//		return types;
//	}

	/*
	 * Getter and Setter
	 */
}
