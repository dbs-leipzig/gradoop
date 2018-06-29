package org.gradoop.flink.io.impl.rdbms.metadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.TableTuple;

public class SQLQuery {

	public static String getNodeTableQuery(String tableName, ArrayList<TableTuple> primaryKeys,
			LinkedHashMap<TableTuple, String> foreignKeys, ArrayList<TableTuple> furtherAttributes) {
		String sqlQuery = "SELECT ";
		int i = 0;
		for (TableTuple pk : primaryKeys) {
			sqlQuery = sqlQuery + pk.f0 + ",";
			i++;
		}
		for (Entry<TableTuple, String> fk : foreignKeys.entrySet()) {
			sqlQuery += fk.getKey() + ",";
			i++;
		}
		for (TableTuple att : furtherAttributes) {
			sqlQuery += att + ",";
			i++;
		}
		return sqlQuery = sqlQuery.substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
	}
	
	public static String getEdgeTableQuery(String tableName, ArrayList<TableTuple> primaryKeys,
			LinkedHashMap<TableTuple, String> foreignKeys, ArrayList<TableTuple> furtherAttributes) {
		String sqlQuery = "SELECT ";
		
		return sqlQuery;
	}
}
