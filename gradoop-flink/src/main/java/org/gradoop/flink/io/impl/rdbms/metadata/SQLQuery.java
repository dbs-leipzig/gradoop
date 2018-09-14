/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.flink.io.impl.rdbms.metadata;

import java.util.ArrayList;
import org.gradoop.flink.io.impl.rdbms.connection.RdbmsConfig;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.tuples.FkTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;

/**
 * Provides sql queries
 */
public class SQLQuery {

	/**
	 * Creates a sql query for vertex conversion
	 * 
	 * @param tableName
	 *            Name of database table
	 * @param primaryKeys
	 *            List of primary keys
	 * @param foreignKeys
	 *            List of foreign keys
	 * @param furtherAttributes
	 *            List of further attributes
	 * @return Valid sql string for querying needed data for tuple-to-vertex
	 *         conversation
	 */
	public static String getNodeTableQuery(String tableName, ArrayList<NameTypeTuple> primaryKeys,
			ArrayList<FkTuple> foreignKeys, ArrayList<NameTypeTuple> furtherAttributes) {

		String sqlQuery = "SELECT ";

		for (NameTypeTuple pk : primaryKeys) {
			if (RdbmsConfig.rdbmsType == RdbmsConstants.SQLSERVER_TYPE_ID) {
				sqlQuery = sqlQuery + "[" + pk.f0 + "]" + ",";
			} else {
				sqlQuery = sqlQuery + pk.f0 + ",";
			}
		}

		for (FkTuple fk : foreignKeys) {
			if (RdbmsConfig.rdbmsType == RdbmsConstants.SQLSERVER_TYPE_ID) {
				sqlQuery = sqlQuery + "[" + fk.f0 + "]" + ",";
			} else {
				sqlQuery += fk.f0 + ",";
			}
		}

		for (NameTypeTuple att : furtherAttributes) {
			if (RdbmsConfig.rdbmsType == RdbmsConstants.SQLSERVER_TYPE_ID) {
				sqlQuery = sqlQuery + "[" + att.f0 + "]" + ",";
			} else {
				sqlQuery += att.f0 + ",";
			}
		}

		return sqlQuery.substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
	}

	/**
	 * Creates a sql query for tuple to edge conversation
	 * 
	 * @param tableName
	 *            Name of database table
	 * @param startAttribute
	 *            Name of first foreign key attribute
	 * @param endAttribute
	 *            Name of second foreign key attribute
	 * @param furtherAttributes
	 *            List of further attributes
	 * @return Valid sql string for querying needed data for tuple-to-edge
	 *         conversation
	 */
	public static String getNtoMEdgeTableQuery(String tableName, String startAttribute, String endAttribute,
			ArrayList<NameTypeTuple> furtherAttributes) {

		if (RdbmsConfig.rdbmsType == RdbmsConstants.SQLSERVER_TYPE_ID) {
			startAttribute = "[" + startAttribute + "]";
			endAttribute = "[" + endAttribute + "]";
		}

		String sqlQuery = "SELECT " + startAttribute + "," + endAttribute + ",";

		for (NameTypeTuple att : furtherAttributes) {
			if (RdbmsConfig.rdbmsType == RdbmsConstants.SQLSERVER_TYPE_ID) {
				sqlQuery += "[" + att.f0 + "]" + ",";
			} else {
				sqlQuery += att.f0 + ",";
			}
		}

		return sqlQuery.substring(0, sqlQuery.length() - 1) + " FROM " + tableName;
	}
}
