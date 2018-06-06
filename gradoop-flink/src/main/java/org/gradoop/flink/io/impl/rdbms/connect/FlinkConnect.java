package org.gradoop.flink.io.impl.rdbms.connect;

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
import org.gradoop.flink.io.impl.rdbms.functions.MapTypeInformation;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;

import com.fasterxml.jackson.databind.introspect.TypeResolutionContext.Basic;

public class FlinkConnect {

	public FlinkConnect() {
	}

	public static DataSet<Row> connect(ExecutionEnvironment env, Tuple3<String,String,String> rdbmsConfig, RDBMSTable table, String tableoredge) throws ClassNotFoundException {

		int parallelism = env.getParallelism();
		int partitionNumber = table.getNumberOfRows() / parallelism;
		int partitionRest = table.getNumberOfRows() % parallelism;
		JDBCInputFormat jdbcInput = null;

		Serializable[][] parameters = new Integer[parallelism][2];
		int j = 0;
		for (int i = 0; i < parallelism; i++) {
			if (i == parallelism - 1) {
				parameters[i] = new Integer[] { j, partitionNumber + partitionRest };
			} else {
				parameters[i] = new Integer[] { j, partitionNumber };
				j = j + partitionNumber;
			}
		}

		if(tableoredge.equals(RDBMSConstants.NODE_TABLE)){
			System.out.println(table.getTableName() + " : " + parallelism + " : " + partitionNumber);
			jdbcInput = JDBCInputFormat
					.buildJDBCInputFormat()
					.setDrivername(RDBMSConstants.DRIVER)
					.setDBUrl(rdbmsConfig.f0)
					.setUsername(rdbmsConfig.f1)
					.setPassword(rdbmsConfig.f2)
					.setQuery(table.getNodeSqlQuery() + " limit ?,?")
					.setRowTypeInfo(new MapTypeInformation().getRowTypeInfo(table))
					.setParametersProvider(new GenericParameterValuesProvider(parameters))
					.finish();
		}
		if(tableoredge.equals(RDBMSConstants.EDGE_TABLE)){
			System.out.println(table.getTableName() + " : " + parallelism + " : " + partitionNumber);
			jdbcInput = JDBCInputFormat
					.buildJDBCInputFormat()
					.setDrivername(RDBMSConstants.DRIVER)
					.setDBUrl(rdbmsConfig.f0)
					.setUsername(rdbmsConfig.f1)
					.setPassword(rdbmsConfig.f2)
					.setQuery(table.getEdgeSqlQuery() + " limit ?,?")
					.setRowTypeInfo(new MapTypeInformation().getRowTypeInfo(table))
					.setParametersProvider(new GenericParameterValuesProvider(parameters))
					.finish();
		}
		return env.createInput(jdbcInput);
	}
}


