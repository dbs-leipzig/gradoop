package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.List;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.io.impl.rdbms.tuples.SchemaType;

public class TableTuplesToNodesEdges extends RichMapFunction<Tuple2<Tuple3<GradoopId,GradoopId,Properties>, Tuple2<GradoopId,GradoopId>>, Edge> {

	private VertexFactory vertexFactory;
	private String tableName;

	public TableTuplesToNodesEdges(VertexFactory vertexFactory, String tableName) {
		this.vertexFactory = vertexFactory;
		this.tableName = tableName;
	}

	@Override
	public Edge map(Tuple2<Tuple3<GradoopId,GradoopId,Properties>, Tuple2<GradoopId,GradoopId>> tuple) throws Exception {
		// TODO Auto-generated method stub
		Edge e = new Edge();
		e.setId(tuple.f0.f0);
		e.setSourceId(tuple.f0.f1);
		e.setTargetId(tuple.f1.f1);
		e.setLabel(tableName);
		e.setProperties(tuple.f0.f2);
		return e;
	}
}
