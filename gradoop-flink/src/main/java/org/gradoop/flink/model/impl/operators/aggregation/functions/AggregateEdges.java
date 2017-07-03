
/**
 * edge,.. => aggregateValue
 */
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Aggregate function.
 */
public class AggregateEdges
  implements GroupCombineFunction<Edge, PropertyValue> {

  /**
   * Aggregate function.
   */
  private final EdgeAggregateFunction aggFunc;

  /**
   * Constructor.
   *
   * @param aggFunc aggregate function
   */
  public AggregateEdges(EdgeAggregateFunction aggFunc) {
    this.aggFunc = aggFunc;
  }

  @Override
  public void combine(
    Iterable<Edge> edges, Collector<PropertyValue> out) throws Exception {
    PropertyValue aggregate = null;

    for (Edge edge : edges) {
      PropertyValue increment = aggFunc.getEdgeIncrement(edge);
      if (increment != null) {
        if (aggregate == null) {
          aggregate = increment;
        } else {
          aggregate = aggFunc.aggregate(aggregate, increment);
        }
      }
    }

    if (aggregate != null) {
      out.collect(aggregate);
    }
  }
}
