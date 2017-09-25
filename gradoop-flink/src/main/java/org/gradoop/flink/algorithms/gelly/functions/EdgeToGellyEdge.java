package org.gradoop.flink.algorithms.gelly.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Convert a Gradoop {@link Edge} to a Gelly Edge.
 *
 * @param <E> Type of the output Gelly Edge.
 */
public interface EdgeToGellyEdge<E> extends MapFunction<Edge, org.apache.flink.graph.Edge<GradoopId, E>> {
}
