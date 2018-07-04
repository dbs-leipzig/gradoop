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
import org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing.TableToNode;

/**
 * converts tuples of sql query result to vertices
 * @author pc
 *
 */
public class CreateVertices extends RichMapFunction<Row, Vertex> {
	// tables to nodes metadata
	private List<TableToNode> tables;
	private int tablePos;
	private TableToNode currentTable;
	
	public CreateVertices(int tablePos){
		this.tablePos = tablePos;
	}
	
	@Override
	public Vertex map(Row tuple) throws Exception {
		// TODO Auto-generated method stub
		this.currentTable = tables.get(tablePos);
		RowHeader rowheader = currentTable.getRowheader();
		String pkString = new PrimaryKeyConcatString().getPrimaryKeyString(tuple,rowheader);
		
		Vertex v = new Vertex();
		v.setId(GradoopId.get());
		v.setLabel(currentTable.getTableName());
		v.setProperties(AttributesToProperties.getProperties(tuple, rowheader));
		v.getProperties().set("PrimaryKey",pkString);
		return v;
	}

	public void open(Configuration parameters) throws Exception {
		this.tables = getRuntimeContext().getBroadcastVariable("tables");
	}
}
