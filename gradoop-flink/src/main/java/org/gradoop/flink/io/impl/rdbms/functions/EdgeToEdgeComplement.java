package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Creates edges with opposite direction
 */
public class EdgeToEdgeComplement implements MapFunction<Edge,Edge> {

	@Override
	public Edge map(Edge e1) throws Exception {
		Edge e2 = new Edge();
		e2.setId(GradoopId.get());
		e2.setLabel(e1.getLabel());
		e2.setSourceId(e1.getTargetId());
		e2.setTargetId(e1.getSourceId());
		e2.setProperties(e1.getProperties());
		return e2;
	}
}
