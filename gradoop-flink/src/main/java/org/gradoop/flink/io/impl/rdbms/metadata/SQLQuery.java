package org.gradoop.flink.io.impl.rdbms.metadata;

import java.util.ArrayList;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTypeTuple;

/**
 * Provides valid sql strings for querying needed relational data
 */
public class SQLQuery {

	/**
	 * Creates a sql query for vertex conversion
	 * @param tableName Name of database table
	 * @param primaryKeys List of primary keys
	 * @param foreignKeys List of foreign keys
	 * @param furtherAttributes List of further attributes
	 * @return Valid sql string for querying needed data for tuple-to-vertex conversation
	 */
	public static String getNodeTableQuery(String tableName, ArrayList<NameTypeTuple> primaryKeys, ArrayList<Tuple2<NameTypeTuple,String>> foreignKeys,
			ArrayList<NameTypeTypeTuple> furtherAttributes) {
		
		String sqlQuery = "SELECT ";
		
		for (NameTypeTuple pk : primaryKeys) {
			sqlQuery = sqlQuery + pk.f0 + ",";
		}
		
		for (Tuple2<NameTypeTuple,String> fk : foreignKeys){
			sqlQuery += fk.f0.f0 + ",";
		}
		
		for (NameTypeTypeTuple att : furtherAttributes) {
			sqlQuery += att.f0 + ",";
		}
		
		return sqlQuery.substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
	}

	/**
	 * Creates a sql query for tuple to edge conversation
	 * @param tableName Name of database table
	 * @param startAttribute Name of first foreign key attribute
	 * @param endAttribute Name of second foreign key attribute
	 * @param furtherAttributes List of further attributes
	 * @return Valid sql string for querying needed data for tuple-to-edge conversation
	 */
	public static String getNtoMEdgeTableQuery(String tableName, String startAttribute, String endAttribute,
			ArrayList<NameTypeTypeTuple> furtherAttributes) {
		
		String sqlQuery = "SELECT " + startAttribute + "," + endAttribute + ",";
		
		for (NameTypeTypeTuple att : furtherAttributes) {
			sqlQuery += att.f0 + ",";
		}
		
		return sqlQuery.substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
	}
}
