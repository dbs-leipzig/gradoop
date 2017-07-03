
package org.gradoop.flink.model.impl.operators.aggregation.functions.containment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.bool.Or;

/**
 * superclass of aggregate and filter functions that check vertex or edge label
 * presence in a graph.
 *
 * Usage: First, aggregate and, second, filter using the same UDF instance.
 */
public abstract class HasLabel extends Or
  implements AggregateFunction, FilterFunction<GraphHead> {

  /**
   * label to check presence of
   */
  protected final String label;

  /**
   * Constructor.
   *
   * @param label label to check presence of
   */
  public HasLabel(String label) {
    this.label = label;
  }

  @Override
  public boolean filter(GraphHead graphHead) throws Exception {
    return graphHead.getPropertyValue(getAggregatePropertyKey()).getBoolean();
  }
}
