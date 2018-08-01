package org.gradoop.flink.io.impl.rdbms.connection;

import java.io.Serializable;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.split.GenericParameterValuesProvider;
import org.apache.flink.api.java.operators.DataSource;
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
	public static DataSet<Row> connect(ExecutionEnvironment env, RDBMSConfig rdbmsConfig, int rowCount, String sqlQuery,
			RowTypeInfo typeInfo) throws ClassNotFoundException {

		// used for best database pagination
		int parallelism = env.getParallelism();
		int partitionNumber = rowCount / parallelism;
		int partitionRest = rowCount % parallelism;

		// computes the parameter array for ParametersProvider
		Serializable[][] parameters = new Integer[parallelism][2];
		int j = 0;
		for (int i = 0; i < parallelism; i++) {
			if (i == parallelism - 1) {
				parameters[i] = new Integer[] { j, partitionNumber + partitionRest };
				System.out.println(parameters[i][0] + "," + parameters[i][1]);
			} else {
				parameters[i] = new Integer[] { j, partitionNumber};
				j = j + partitionNumber;
				System.out.println(parameters[i][0] + "," + parameters[i][1]);
			}
		}
		
		System.out.println(sqlQuery+pageinationQuery(rdbmsConfig.getUrl()));
		// run jdbc input format with pagination
		JDBCInputFormat jdbcInput = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.gradoop.flink.io.impl.rdbms.connection.DriverShim").setDBUrl(rdbmsConfig.getUrl())
				.setUsername(rdbmsConfig.getUser()).setPassword(rdbmsConfig.getPw())
				.setQuery(sqlQuery + pageinationQuery(rdbmsConfig.getUrl())).setRowTypeInfo(typeInfo)
				.setParametersProvider(new GenericParameterValuesProvider(parameters)).finish();

		return env.createInput(jdbcInput);
	}

	public static String pageinationQuery(String url) {
		
		// default pageination query for mysql,mariadb,postgres, ... management systems
		String pageinationQuery = " limit ? offset ?";
		
		// pageination query for microsoft sql server management system
		if(url.contains("sqlserver")) {
			pageinationQuery = " ORDER BY 1 OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";
		}
		
		return pageinationQuery;
	}
}
