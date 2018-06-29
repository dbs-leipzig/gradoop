package org.gradoop.flink.io.impl.rdbms.metadata;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.gradoop.flink.io.impl.rdbms.tuples.TableTuple;

public class RDBMSTableBase {
	private String tableName;
	private boolean toNodesTable;
	private ArrayList<TableTuple> primaryKeys;
	private LinkedHashMap<TableTuple, String> foreignKeys;
	private ArrayList<TableTuple> furtherAttributes;
	private boolean directionIndicator;
	private int rowCount;

	public RDBMSTableBase(String tableName, boolean toNodesTable, ArrayList<TableTuple> primaryKeys,
			LinkedHashMap<TableTuple, String> foreignKeys, ArrayList<TableTuple> furtherAttributes,
			boolean directionIndicator, int rowCount) {
		this.tableName = tableName;
		this.toNodesTable = toNodesTable;
		this.primaryKeys = primaryKeys;
		this.foreignKeys = foreignKeys;
		this.furtherAttributes = furtherAttributes;
		this.directionIndicator = directionIndicator;
		this.rowCount = rowCount;
	}

	public String getSqlQuery() {
		String sqlQuery = "";
		if (toNodesTable) {
			sqlQuery = SQLQuery.getNodeTableQuery(tableName, primaryKeys, foreignKeys, furtherAttributes);
		} else {
			sqlQuery = SQLQuery.getEdgeTableQuery(tableName, primaryKeys, foreignKeys, furtherAttributes);
		}
		return sqlQuery;
	}

	public ArrayList<JDBCType> getTypes() {
		ArrayList<JDBCType> types = new ArrayList<JDBCType>();
		for (TableTuple pks : primaryKeys) {
			types.add(pks.f3);
		}
		if (!foreignKeys.isEmpty()) {
			for (Entry<TableTuple, String> fks : foreignKeys.entrySet()) {
				types.add(fks.getKey().f3);
			}
		}
		if (!furtherAttributes.isEmpty()) {
			for (TableTuple att : furtherAttributes) {
				types.add(att.f3);
			}
		}
		return types;
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

	public boolean isToNodesTable() {
		return toNodesTable;
	}

	public void setToNodesTable(boolean toNodesTable) {
		this.toNodesTable = toNodesTable;
	}

	public ArrayList<TableTuple> getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(ArrayList<TableTuple> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public LinkedHashMap<TableTuple, String> getForeignKeys() {
		return foreignKeys;
	}

	public void setForeignKeys(LinkedHashMap<TableTuple, String> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}

	public ArrayList<TableTuple> getFurtherAttributes() {
		return furtherAttributes;
	}

	public void setFurtherAttributes(ArrayList<TableTuple> furtherAttributes) {
		this.furtherAttributes = furtherAttributes;
	}

	public boolean isDirectionIndicator() {
		return directionIndicator;
	}

	public void setDirectionIndicator(boolean directionIndicator) {
		this.directionIndicator = directionIndicator;
	}

	public int getRowCount() {
		return rowCount;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}
}
