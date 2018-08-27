package org.gradoop.flink.model.impl.operators.sampling.statistics.functions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Writes the sum of vertex degrees as property to the graphHead.
 */
public class AddSumDegreesToGraphHeadCrossFunction
  implements CrossFunction<WithCount<GradoopId>, GraphHead, GraphHead> {

  /**
   * The used property key for the sum of vertex degrees
   */
  private final String propertyKey;

  /**
   * Constructor
   *
   * @param propertyKey The used property key for the sum of vertex degrees
   */
  public AddSumDegreesToGraphHeadCrossFunction(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  /**
   * Writes the sum of vertex degrees as property to the graphHead
   *
   * @param gradoopIdWithCount The {@code WithCount}-Object containing the sum-value
   * @param graphHead The graphHead the sum-value is written to
   * @return The graphHead with the sum-value as property
   */
  @Override
  public GraphHead cross(WithCount<GradoopId> gradoopIdWithCount, GraphHead graphHead) {
    graphHead.setProperty(propertyKey, gradoopIdWithCount.getCount());
    return graphHead;
  }
}
