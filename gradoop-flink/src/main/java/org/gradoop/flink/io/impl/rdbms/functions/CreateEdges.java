package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.List;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeader;

public class CreateEdges extends RichMapFunction<Row,ImportEdge<String>>{
	private List<RDBMSTable> table;
	private List<Vertex> vertices;
	private int tablePos;
	private int fkPos;
	private RowHeader rowHeader;
	
	public CreateEdges(int tablePos,int fkPos){
		this.tablePos = tablePos;
		this.fkPos = fkPos;
	}
	
	@Override
	public ImportEdge<String> map(Row tuple) throws Exception {
		// TODO Auto-generated method stub
		rowHeader = table.get(tablePos).getRowHeader();
		String pkString = new PrimaryKeyConcatString().getPrimaryKeyString(tuple,rowHeader);
		ImportEdge<String> e = new ImportEdge<String>();
		e.setLabel(rowHeader.getForeignKeyHeader().get(fkPos).getName().toString());
		e.setSourceId(pkString);
		e.setTargetId(tuple.getField(rowHeader.getForeignKeyHeader().get(fkPos).getPos()).toString());
		Properties props = new Properties();
		e.setProperties(props);
		return e;
	}
	
	public void open(Configuration parameters) throws Exception {
		this.table = getRuntimeContext().getBroadcastVariable("tables");
		this.vertices = getRuntimeContext().getBroadcastVariable("vertices");
	}
}
