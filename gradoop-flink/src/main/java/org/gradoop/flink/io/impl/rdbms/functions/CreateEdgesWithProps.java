package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeader;

public class CreateEdgesWithProps extends RichMapFunction<Row,ImportEdge<String> > {
	List<RDBMSTable> table;
	int tablePos;
	
	public CreateEdgesWithProps(int tablePos){
		this.tablePos = tablePos;
	}

	@Override
	public ImportEdge<String> map(Row tuple) throws Exception {
		// TODO Auto-generated method stub
		RowHeader rowHeader = table.get(tablePos).getRowHeader();
		ImportEdge e = new ImportEdge();
		e.setLabel(table.get(tablePos).getTableName());
		e.setSourceId(tuple.getField(rowHeader.getForeignKeyHeader().get(0).getPos()).toString());
		e.setTargetId(tuple.getField(rowHeader.getForeignKeyHeader().get(1).getPos()).toString());
		e.setProperties(AttributesToProperties.getProperties(tuple, rowHeader));
		return e;
	}
	
	public void open(Configuration parameters) throws Exception {
		this.table = getRuntimeContext().getBroadcastVariable("tables");
	}
}
