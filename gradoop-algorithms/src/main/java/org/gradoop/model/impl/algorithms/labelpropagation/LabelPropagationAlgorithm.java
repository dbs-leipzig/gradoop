package org.gradoop.model.impl.algorithms.labelpropagation;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.NullValue;
import org.gradoop.model.impl.algorithms.labelpropagation.functions.LPMessageFunction;
import org.gradoop.model.impl.algorithms.labelpropagation.functions.LPUpdateFunction;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Implementation of the Label Propagation Algorithm.
 *
 * In each super step each vertex will adopt the value sent by the majority of
 * their neighbors or the smallest one if there is just one neighbor. If
 * multiple labels occur with the same frequency, the minimum of them will be
 * selected as new label. If a vertex changes its value in a super step, the new
 * value will be propagated to the neighbours.
 *
 * The computation will terminate if no new values are assigned.
 */
public class LabelPropagationAlgorithm implements GraphAlgorithm
  <GradoopId, PropertyValue, NullValue,
    Graph<GradoopId, PropertyValue, NullValue>> {

  /**
   * Counter to define maximum number of iterations for the algorithm
   */
  private int maxIterations;

  /**
   * Constructor
   *
   * @param maxIterations int counter to define maximum number of iterations
   */
  public LabelPropagationAlgorithm(int maxIterations) {
    this.maxIterations = maxIterations;
  }

  @Override
  public Graph<GradoopId, PropertyValue, NullValue> run(
    Graph<GradoopId, PropertyValue, NullValue> graph) throws Exception {
    return graph.runVertexCentricIteration(
      new LPUpdateFunction(),
      new LPMessageFunction(),
      maxIterations);
  }
}
