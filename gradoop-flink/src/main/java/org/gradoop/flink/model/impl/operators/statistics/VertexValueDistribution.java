
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Extracts an arbitrary value (e.g. label, property key/value, ...) from a vertex and computes its
 * distribution among all vertices.
 *
 * @param <T> value type
 */
public class VertexValueDistribution<T> extends ValueDistribution<Vertex, T> {
  /**
   * Constructor
   *
   * @param valueFunction extracts a value from a vertex
   */
  public VertexValueDistribution(MapFunction<Vertex, T> valueFunction) {
    super(valueFunction);
  }

  @Override
  public DataSet<WithCount<T>> execute(LogicalGraph graph) {
    return compute(graph.getVertices());
  }
}
