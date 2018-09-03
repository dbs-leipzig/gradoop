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
package org.gradoop.flink.io.impl.rdbms.connection;

import java.io.Serializable;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.split.GenericParameterValuesProvider;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

/**
 * Queries the relational data
 */
public class FlinkConnect {

	/**
	 * Connects to a relational database and querying data in a distributed manner.
	 * 
	 * @param env
	 *            Flink Execution Environment
	 * @param rdbmsConfig
	 *            Configuration of the used database management system
	 * @param rowCount
	 *            Number of table rows
	 * @param sqlQuery
	 *            Valid sql query
	 * @param typeInfo
	 *            Database row type information
	 * @return DataSet of type row, consisting of queried relational data
	 * @throws ClassNotFoundException
	 */
	public static DataSet<Row> connect(ExecutionEnvironment env, RdbmsConfig rdbmsConfig, int rowCount, String sqlQuery,
			RowTypeInfo typeInfo) throws ClassNotFoundException {

		int parallelism = env.getParallelism();

		// run jdbc input format with pagination
		JDBCInputFormat jdbcInput = null;
		try {
			jdbcInput = JDBCInputFormat.buildJDBCInputFormat()
					.setDrivername("org.gradoop.flink.io.impl.rdbms.connection.DriverShim")
					.setDBUrl(rdbmsConfig.getUrl()).setUsername(rdbmsConfig.getUser()).setPassword(rdbmsConfig.getPw())
					.setQuery(sqlQuery + pageinationQuery(rdbmsConfig.getUrl())).setRowTypeInfo(typeInfo)
					.setParametersProvider(
							new GenericParameterValuesProvider(parameters(parallelism, rowCount, rdbmsConfig.getUrl())))
					.finish();
		} catch (Exception e) {
			System.out.println("No propper typeparsing");
		}
		return env.createInput(jdbcInput);
	}

	// chooses the right pageination query for belonging management system
	public static String pageinationQuery(String url) {

		// default pageination query for mysql,mariadb,postgres, ... management
		// systems
		String pageinationQuery = " LIMIT ? OFFSET ?";

		// pageination query for microsoft sql server management system
		if (url.contains(":sqlserver:")) {
			pageinationQuery = " ORDER BY 1 OFFSET (?) ROWS FETCH NEXT (?) ROWS ONLY";
		}

		return pageinationQuery;
	}

	// chooses the right parameters for pageination query
	public static Serializable[][] parameters(int parallelism, int rowCount, String url) {

		// splits database data in parts of same
		int partitionNumber = rowCount / parallelism;
		int partitionRest = rowCount % parallelism;

		Serializable[][] parameters = new Integer[parallelism][2];
		int j = 0;
		
		if (url.contains(":sqlserver:")) {
			for (int i = 0; i < parallelism; i++) {
				if (i == parallelism - 1) {
					parameters[i] = new Integer[] { j, partitionNumber + partitionRest };
				} else {
					if (partitionNumber == 0) {
						partitionNumber = 1;
					}
					parameters[i] = new Integer[] { j, partitionNumber };

					j = j + partitionNumber;
				}
			}
		} else {			
			for (int i = 0; i < parallelism; i++) {
				if (i == parallelism - 1) {
					parameters[i] = new Integer[] { partitionNumber + partitionRest, j };
				} else {
					parameters[i] = new Integer[] { partitionNumber, j };
					j = j + partitionNumber;
				}
			}
		}
		
		return parameters;
	}
}
