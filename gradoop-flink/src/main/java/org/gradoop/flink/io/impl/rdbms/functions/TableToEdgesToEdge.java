package org.gradoop.flink.io.impl.rdbms.functions;

import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToEdge;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class TableToEdgesToEdge extends RichFlatMapFunction<TableToEdge, Edge> {
	private List<Vertex> vertices;
	private EPGMEdgeFactory edgeFactory;

	public TableToEdgesToEdge(GradoopFlinkConfig config) {
		this.edgeFactory = config.getEdgeFactory();
	}

	@Override
	public void flatMap(TableToEdge table, Collector<Edge> out) throws Exception {
		String label = table.getStartAttribute().f0;
		GradoopId id;
		String key;

		for (Vertex v : vertices) {
			if (v.getLabel().equals(table.getendTableName())) {
				id = v.getId();
				key = v.getProperties().get(table.getEndAttribute().f0).toString();
				for (Vertex fkVertice : vertices) {
					if (v.getLabel().equals(table.getstartTableName())) {
						GradoopId fkId = fkVertice.getId();
						out.collect((Edge) edgeFactory.initEdge(GradoopId.get(), label, fkId, id));
					}
				}
			}
		}
	}

	public void open(Configuration parameters) throws Exception {
		this.vertices = getRuntimeContext().getBroadcastVariable("vertices");
	}
}
