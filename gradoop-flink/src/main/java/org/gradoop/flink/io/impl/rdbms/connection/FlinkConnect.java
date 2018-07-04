package org.gradoop.flink.io.impl.rdbms.connection;

import java.io.Serializable;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.split.GenericParameterValuesProvider;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

/**
 * connects to a RDBMS via Flinks' JDBC Input Format
 * 
 * @author pc
 *
 */
public class FlinkConnect {

	public FlinkConnect() {
	}

	/**
	 * computes best pageination, connects to rdbms via jdbc and run sql query
	 * 
	 * @param env
	 * @param rdbmsConfig
	 * @param table
	 * @param tableoredge
	 * @return
	 * @throws ClassNotFoundException
	 */
	public static DataSet<Row> connect(ExecutionEnvironment env, RDBMSConfig rdbmsConfig, int rowCount, String sqlQuery,
			RowTypeInfo typeInfo) throws ClassNotFoundException {

		// parallelism of running cluster or local mashine
		int parallelism = env.getParallelism();

		// divides table data into equal parts
		int partitionNumber = rowCount / parallelism;
		int partitionRest = rowCount % parallelism;

		JDBCInputFormat jdbcInput = null;

		/*
		 * provides parameters for sql query
		 */
		Serializable[][] parameters = new Integer[parallelism][2];
		int j = 0;
		for (int i = 0; i < parallelism; i++) {
			if (i == parallelism - 1) {
				parameters[i] = new Integer[] { partitionNumber + partitionRest, j };
			} else {
				parameters[i] = new Integer[] { partitionNumber, j };
				j = j + partitionNumber;
			}
		}

		jdbcInput = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.gradoop.flink.io.impl.rdbms.connection.DriverShim").setDBUrl(rdbmsConfig.getUrl())
				.setUsername(rdbmsConfig.getUser()).setPassword(rdbmsConfig.getPw()).setQuery(sqlQuery + " limit ? offset ?")
				.setRowTypeInfo(typeInfo).setParametersProvider(new GenericParameterValuesProvider(parameters))
				.finish();

		return env.createInput(jdbcInput);
	}
}
