package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

import java.util.Iterator;

public class AggregateVertices
  implements GroupCombineFunction<Vertex, PropertyValue> {

  private final VertexAggregateFunction aggFunc;

  public AggregateVertices(VertexAggregateFunction aggFunc) {
    this.aggFunc = aggFunc;
  }

  @Override
  public void combine(
    Iterable<Vertex> vertices, Collector<PropertyValue> out) throws Exception {
    PropertyValue aggregate = null;

    for (Vertex vertex : vertices) {
      PropertyValue increment = aggFunc.getVertexIncrement(vertex);
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
