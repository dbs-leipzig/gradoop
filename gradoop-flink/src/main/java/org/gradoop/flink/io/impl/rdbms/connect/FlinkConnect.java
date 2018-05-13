package org.gradoop.flink.io.impl.rdbms.connect;

import java.io.Serializable;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.split.GenericParameterValuesProvider;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.functions.MapTypeInformation;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;

import com.fasterxml.jackson.databind.introspect.TypeResolutionContext.Basic;

public class FlinkConnect {
	public FlinkConnect() {
	}

	public static DataSet<Row> connect(ExecutionEnvironment env, RDBMSTable table) throws ClassNotFoundException {
		
		Serializable[][] queryParameters = new String[2][1];
		queryParameters[0] = new String[]{"0, 10"};
		queryParameters[1] = new String[]{"11,20"};
		
		return env.createInput(
				JDBCInputFormat
				.buildJDBCInputFormat()
				.setDrivername(RDBMSConstants.DRIVER)
				.setDBUrl(RDBMSConstants.URL)
				.setUsername(RDBMSConstants.USER)
				.setPassword(RDBMSConstants.PW)
				.setQuery("select * from " + table.getTableName()+" limit ?")
				.setRowTypeInfo(new MapTypeInformation().getRowTypeInfo(table))
				.setParametersProvider(new GenericParameterValuesProvider(queryParameters))
				.finish()
				);
	}
}
