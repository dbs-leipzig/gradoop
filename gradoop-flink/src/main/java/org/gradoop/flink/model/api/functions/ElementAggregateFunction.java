package org.gradoop.flink.model.api.functions;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;

/**
 * Describes a element aggregate function as input for the {@link Aggregation} operator.
 */
public interface ElementAggregateFunction extends AggregateFunction<Element> {

  @Override
  default boolean aggregatesVertices() {
    return true;
  }

  @Override
  default boolean aggregatesEdges() {
    return true;
  }
}
