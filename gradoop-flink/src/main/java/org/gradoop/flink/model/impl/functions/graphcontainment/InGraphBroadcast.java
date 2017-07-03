
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.GraphElement;

/**
 * True, if an element is not contained in a given graph.
 *
 * @param <GE> element type
 */
@FunctionAnnotation.ReadFields("graphIds")
public class InGraphBroadcast<GE extends GraphElement>
  extends GraphContainmentFilterBroadcast<GE> {

  @Override
  public boolean filter(GE element) throws Exception {
    return element.getGraphIds().contains(graphId);
  }
}
