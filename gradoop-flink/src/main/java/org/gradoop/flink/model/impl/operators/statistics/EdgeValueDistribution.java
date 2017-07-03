
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Extracts an arbitrary value (e.g. label, property key/value, ...) from an edge and computes its
 * distribution among all edges.
 *
 * @param <T> value type
 */
public class EdgeValueDistribution<T> extends ValueDistribution<Edge, T> {
  /**
   * Constructor
   *
   * @param valueFunction extracts a value from a edge
   */
  public EdgeValueDistribution(MapFunction<Edge, T> valueFunction) {
    super(valueFunction);
  }

  @Override
  public DataSet<WithCount<T>> execute(LogicalGraph graph) {
    return compute(graph.getEdges());
  }
}
