package org.gradoop.flink.io.impl.rdbms.connection;

import java.io.Serializable;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.split.GenericParameterValuesProvider;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSConfig;

import com.fasterxml.jackson.databind.introspect.TypeResolutionContext.Basic;

/**
 * connects to a RDBMS via Flinks' JDBC Input Format
 * @author pc
 *
 */
public class FlinkConnect {

	public FlinkConnect() {
	}

	/**
	 * computes best pageination, connects to rdbms via jdbc and run sql query 
	 * @param env
	 * @param rdbmsConfig
	 * @param table
	 * @param tableoredge
	 * @return
	 * @throws ClassNotFoundException
	 */
	public static DataSet<Row> connect(ExecutionEnvironment env,RDBMSConfig rdbmsConfig, RDBMSTable table, String tableoredge) throws ClassNotFoundException {

		// parallelism of running cluster or local mashine
		int parallelism = env.getParallelism();
		
		// divides table data into equal parts
		int partitionNumber = table.getNumberOfRows() / parallelism;
		int partitionRest = table.getNumberOfRows() % parallelism;
		
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

		/*
		 * provides a jdbc input for tables convert to nodes
		 */
		if(tableoredge.equals(RDBMSConstants.NODE_TABLE)){
			jdbcInput = JDBCInputFormat
					.buildJDBCInputFormat()
					.setDrivername("org.gradoop.flink.io.impl.rdbms.jdbcdriver.DriverShim")
					.setDBUrl(rdbmsConfig.f0)
					.setUsername(rdbmsConfig.f1)
					.setPassword(rdbmsConfig.f2)
					.setQuery(table.getNodeSqlQuery() + " limit ? offset ?")
					.setRowTypeInfo(new MapTypeInformation().getRowTypeInfo(table))
					.setParametersProvider(new GenericParameterValuesProvider(parameters))
					.finish();
		}
		
		/*
		 * provides a jdbc input for a tables convert to edges
		 */
		if(tableoredge.equals(RDBMSConstants.EDGE_TABLE)){
			jdbcInput = JDBCInputFormat
					.buildJDBCInputFormat()
					.setDrivername("org.gradoop.flink.io.impl.rdbms.jdbcdriver.DriverShim")
					.setDBUrl(rdbmsConfig.f0)
					.setUsername(rdbmsConfig.f1)
					.setPassword(rdbmsConfig.f2)
					.setQuery(table.getEdgeSqlQuery() + " limit ? offset ?")
					.setRowTypeInfo(new MapTypeInformation().getRowTypeInfo(table))
					.setParametersProvider(new GenericParameterValuesProvider(parameters))
					.finish();
		}
		return env.createInput(jdbcInput);
	}
}


