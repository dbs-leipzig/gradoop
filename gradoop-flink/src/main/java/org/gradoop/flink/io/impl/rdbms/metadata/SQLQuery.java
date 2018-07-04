package org.gradoop.flink.io.impl.rdbms.metadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.io.impl.rdbms.tuples.AttTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.PKTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.TableTuple;

public class SQLQuery {

	public static String getNodeTableQuery(String tableName, ArrayList<NameTypeTuple> primaryKeys, ArrayList<Tuple2<NameTypeTuple,String>> foreignKeys,
			ArrayList<NameTypeTuple> furtherAttributes) {
		String sqlQuery = "SELECT ";
		for (NameTypeTuple pk : primaryKeys) {
			sqlQuery = sqlQuery + pk.f0 + ",";
		}
		for (Tuple2<NameTypeTuple,String> fk : foreignKeys){
			sqlQuery += fk.f0.f0 + ",";
		}
		
		for (NameTypeTuple att : furtherAttributes) {
			sqlQuery += att.f0 + ",";
		}
		return sqlQuery.substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
	}

	public static String getNtoMEdgeTableQuery(String tableName, String startAttribute, String endAttribute,
			ArrayList<NameTypeTuple> furtherAttributes) {
		String sqlQuery = "SELECT " + startAttribute + "," + endAttribute + ",";
		int i = 0;
		for (NameTypeTuple att : furtherAttributes) {
			sqlQuery += att.f0 + ",";
			i++;
		}
		return sqlQuery.substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
	}
}
