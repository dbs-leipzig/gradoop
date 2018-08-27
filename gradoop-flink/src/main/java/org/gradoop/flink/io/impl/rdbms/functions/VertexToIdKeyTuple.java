package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.List;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToEdge;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;

public class VertexToIdKeyTuple extends RichFlatMapFunction<TableToEdge, Tuple3<GradoopId,GradoopId,String>> {
	List<Vertex> vertices;

	@Override
	public void flatMap(TableToEdge table, Collector<Tuple3<GradoopId,GradoopId,String>> out) throws Exception {
		GradoopId id = null;
		String key = null;
		GradoopId id2;
		String key2;
		GradoopId groupId = GradoopId.get();

		for (Vertex v : vertices) {
			if (v.getLabel().equals(table.getstartTableName())) {
				id = v.getId();
				key = v.getProperties().get(table.getStartAttribute().f0).toString();
				out.collect(new Tuple3<>(groupId, id, key));
			}
			if(v.getLabel().equals(table.getendTableName())){
				id2 = v.getId();
				key2 = v.getProperties().get(table.getEndAttribute().f0).toString();
				out.collect(new Tuple3<>(groupId, id2, key2));
			}
		}
	}

	public void open(Configuration parameters) throws Exception {
		this.vertices = getRuntimeContext().getBroadcastVariable("vertices");
	}
}
