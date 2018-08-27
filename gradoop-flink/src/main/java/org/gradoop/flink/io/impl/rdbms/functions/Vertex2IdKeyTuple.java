package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToEdge;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;

public class Vertex2IdKeyTuple extends RichFlatMapFunction<TableToEdge, IdKeyTuple> {
	List<Vertex> vertices;

	@Override
	public void flatMap(TableToEdge table, Collector<IdKeyTuple> out) throws Exception {
		GradoopId id = null;
		String key = null;

		for (Vertex v : vertices) {
			if (v.getLabel().equals(table.getendTableName())) {
				id = v.getId();
				key = v.getProperties().get(table.getEndAttribute().f0).toString();

				out.collect(new IdKeyTuple(id, key));
			}
		}
	}

	public void open(Configuration parameters) throws Exception {
		this.vertices = getRuntimeContext().getBroadcastVariable("vertices");
	}
}
