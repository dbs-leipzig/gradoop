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
		JDBCInputFormat jdbcInput = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.gradoop.flink.io.impl.rdbms.connection.DriverShim").setDBUrl(rdbmsConfig.getUrl())
				.setUsername(rdbmsConfig.getUser()).setPassword(rdbmsConfig.getPw())
				.setQuery(sqlQuery + PageinationQueryChooser.choose(rdbmsConfig.getRdbmsType()))
				.setRowTypeInfo(typeInfo).setParametersProvider(new GenericParameterValuesProvider(
						ParametersChooser.choose(rdbmsConfig.getRdbmsType(), parallelism, rowCount)))
				.finish();

		return env.createInput(jdbcInput);
	}
}
