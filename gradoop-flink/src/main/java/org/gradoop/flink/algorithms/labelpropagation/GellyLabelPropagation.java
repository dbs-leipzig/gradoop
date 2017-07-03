
package org.gradoop.flink.algorithms.labelpropagation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Executes the label propagation integrated in Flink Gelly.
 */
public class GellyLabelPropagation extends LabelPropagation {

  /**
   * Constructor
   *
   * @param maxIterations Counter to define maximal iteration for the algorithm
   * @param propertyKey   Property key to access the label value
   */
  public GellyLabelPropagation(int maxIterations, String propertyKey) {
    super(maxIterations, propertyKey);
  }

  @Override
  protected DataSet<org.apache.flink.graph.Vertex<GradoopId, PropertyValue>>
  executeInternal(
    Graph<GradoopId, PropertyValue, NullValue> gellyGraph) {
    return new org.apache.flink.graph.library.LabelPropagation
      <GradoopId, PropertyValue, NullValue>(getMaxIterations()).run(gellyGraph);
  }
}
