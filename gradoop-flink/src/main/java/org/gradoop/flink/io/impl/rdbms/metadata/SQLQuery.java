package org.gradoop.flink.io.impl.rdbms.metadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.gradoop.flink.io.impl.rdbms.tuples.AttTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.PKTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.TableTuple;

public class SQLQuery {

	public static String getNodeTableQuery(String tableName, ArrayList<NameTypeTuple> primaryKeys, ArrayList<NameTypeTuple> foreignKeys,
			ArrayList<NameTypeTuple> furtherAttributes) {
		String sqlQuery = "SELECT ";
		for (NameTypeTuple pk : primaryKeys) {
			sqlQuery = sqlQuery + pk.f0 + ",";
		}
		for (NameTypeTuple fk : foreignKeys){
			sqlQuery += sqlQuery + fk.f0 + ",";
		}
		
		for (NameTypeTuple att : furtherAttributes) {
			sqlQuery += att.f0 + ",";
		}
		return sqlQuery = sqlQuery.substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
	}

	public static String getStandardEdgeTableQuery(String startTable, ArrayList<NameTypeTuple> primaryKeys, String endAttribute) {
		String sql = "SELECT ";
		for(NameTypeTuple pk : primaryKeys){
			sql += pk.f0 + ",";
		}
		return sql + endAttribute + " FROM " + startTable;
	}

	public static String getNtoMEdgeTableQuery(String relationshipType, String startAttribute, String endAttribute,
			ArrayList<NameTypeTuple> furtherAttributes) {
		String sqlQuery = "SELECT " + startAttribute + "," + endAttribute + ",";
		int i = 0;
		for (NameTypeTuple att : furtherAttributes) {
			sqlQuery += att.f0 + ",";
			i++;
		}
		return sqlQuery.substring(0, sqlQuery.length() - 1) + " FROM " + relationshipType;
	}
}
