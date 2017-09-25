package org.gradoop.flink.algorithms.gelly.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Convert a Gradoop {@link Vertex} to a Gelly Vertex.
 *
 * @param <E> Type of the output Gelly Vertex.
 */
public interface VertexToGellyVertex<E>
  extends MapFunction<org.gradoop.common.model.impl.pojo.Vertex, Vertex<GradoopId, E>> {
}
