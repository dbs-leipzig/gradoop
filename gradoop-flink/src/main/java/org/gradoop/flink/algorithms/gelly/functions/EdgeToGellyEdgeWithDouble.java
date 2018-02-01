package org.gradoop.flink.algorithms.gelly.functions;

import org.apache.flink.graph.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Maps EPGM edge to a Gelly edge consisting of EPGM source and target
 * identifier and {@link Double} as edge value.
 */
public class EdgeToGellyEdgeWithDouble implements EdgeToGellyEdge<Double> {

	/**
	 * Property key to get the value for.
	 */
	private final String propertyKey;

	/**
	 * Reduce object instantiations.
	 */
	private final org.apache.flink.graph.Edge<GradoopId, Double> reuseEdge;

	public EdgeToGellyEdgeWithDouble(String propertyKey) {
		this.propertyKey = propertyKey;
		this.reuseEdge = new org.apache.flink.graph.Edge<>();
	}

	@Override
	public Edge<GradoopId, Double> map(org.gradoop.common.model.impl.pojo.Edge epgmEdge) throws Exception {
		reuseEdge.setSource(epgmEdge.getSourceId());
		reuseEdge.setTarget(epgmEdge.getTargetId());
		reuseEdge.setValue(epgmEdge.getPropertyValue(propertyKey).getDouble());
		return reuseEdge;
	}

}
