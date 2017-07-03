
package org.gradoop.flink.algorithms.labelpropagation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.flink.algorithms.labelpropagation.functions.LPMessageFunction;
import org.gradoop.flink.algorithms.labelpropagation.functions.LPUpdateFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Executes the label propagation integrated in Gradoop.
 */
public class GradoopLabelPropagation extends LabelPropagation {

  /**
   * Constructor
   *
   * @param maxIterations Counter to define maximal iteration for the algorithm
   * @param propertyKey   Property key to access the label value
   */
  public GradoopLabelPropagation(int maxIterations, String propertyKey) {
    super(maxIterations, propertyKey);
  }

  @Override
  protected DataSet<org.apache.flink.graph.Vertex<GradoopId, PropertyValue>>
  executeInternal(Graph<GradoopId, PropertyValue, NullValue> gellyGraph) {
    return gellyGraph.runScatterGatherIteration(
      new LPMessageFunction(), new LPUpdateFunction(), getMaxIterations())
      .getVertices();
  }
}
