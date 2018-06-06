package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.types.Row;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.impl.rdbms.RDBMSDataSource;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.tuples.SchemaType;

public class TableTuplesToNodes extends RichMapFunction<Row, Vertex> {
	private VertexFactory vertexFactory;
	private List<RDBMSTable> table;
	private int tablePosition;

	public TableTuplesToNodes(VertexFactory vertexFactory, int tablePosition) {
		this.vertexFactory = vertexFactory;
		this.tablePosition = tablePosition;
	}

	@Override
	public Vertex map(Row tuple) throws Exception {
		// TODO Auto-generated method stub
		String pkString = getPrimaryKeyString(tuple);
		Vertex v = new Vertex();
		v.setId(GradoopId.fromString(tuple.getField(tuple.getArity()-1).toString()));
		v.setLabel(table.get(tablePosition).getTableName());
		v.setProperty("PrimaryKey", pkString);
		for(SchemaType attr : table.get(tablePosition).getSimpleAttributes()){
			v.setProperty(attr.getName(), tuple.getField(attr.getPos()).toString());
		}
		for(SchemaType pk : table.get(tablePosition).getPrimaryKey()){
			v.setProperty(pk.getName(), tuple.getField(pk.getPos()).toString());
		}
	
		return v;
		// return
		// vertexFactory.initVertex(GradoopId.get(),table.get(tablePosition).getTableName());
	}

	public String getPrimaryKeyString(Row tuple) {
		String pkString = "";
		for (SchemaType col : table.get(tablePosition).getPrimaryKey()) {
			if (table.get(tablePosition).getPrimaryKey().size() == 1) {
				pkString = tuple.getField(col.getPos()).toString();
			} else {
				pkString = pkString + tuple.getField(col.getPos()).toString() + "#";
			}
		}
		return pkString;
	}

	public void open(Configuration parameters) throws Exception {
		// super.open(parameters);
		this.table = getRuntimeContext().getBroadcastVariable("tables");
	}
}
