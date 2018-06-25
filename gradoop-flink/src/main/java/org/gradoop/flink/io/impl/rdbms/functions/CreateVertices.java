package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.rdbms.metadata.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.metadata.RowHeader;

/**
 * converts tuples of sql query result to vertices
 * @author pc
 *
 */
public class CreateVertices extends RichMapFunction<Row, Vertex> {
	// tables to nodes metadata
	private List<RDBMSTable> table;
	private int tablePosition;
	
	public CreateVertices(int tablePosition){
		this.tablePosition = tablePosition;
	}
	
	@Override
	public Vertex map(Row tuple) throws Exception {
		// TODO Auto-generated method stub
		RowHeader rowHeader = table.get(tablePosition).getRowHeader();
		String pkString = new PrimaryKeyConcatString().getPrimaryKeyString(tuple,rowHeader);
		
		Vertex v = new Vertex();
		v.setId(GradoopId.get());
		v.setLabel(table.get(tablePosition).getTableName());
		v.setProperties(AttributesToProperties.getProperties(tuple, rowHeader));
		v.getProperties().set("PrimaryKey",pkString);
		return v;
	}

	public void open(Configuration parameters) throws Exception {
		this.table = getRuntimeContext().getBroadcastVariable("tables");
	}
}
