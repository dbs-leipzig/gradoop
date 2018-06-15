package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeader;

public class FKandProps extends RichMapFunction<Row, Tuple3<String, String, Properties>> {
	List<RDBMSTable> table;
	int tePos;

	public FKandProps(int tePos) {
		this.tePos = tePos;
	}

	@Override
	public Tuple3<String, String, Properties> map(Row tuple) throws Exception {
		RowHeader rowHeader = table.get(tePos).getRowHeader();
		return new Tuple3(tuple.getField(rowHeader.getForeignKeyHeader().get(0).getPos()).toString(),
				tuple.getField(rowHeader.getForeignKeyHeader().get(1).getPos()).toString(),
				AttributesToProperties.getPropertiesWithoutFKs(tuple, rowHeader));
	}

	public void open(Configuration parameters) throws Exception {
		this.table = getRuntimeContext().getBroadcastVariable("tables");
	}
}
