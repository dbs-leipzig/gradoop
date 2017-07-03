
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;

/**
 * True, if an element is not contained in any of a given set of graphs.
 *
 * @param <GE> element type
 */
@FunctionAnnotation.ReadFields("graphIds")
public class InGraphsBroadcast<GE extends GraphElement>
  extends GraphsContainmentFilterBroadcast<GE> {

  @Override
  public boolean filter(GE element) throws Exception {

    boolean contained = false;

    for (GradoopId graphID : graphIds) {
      contained = element.getGraphIds().contains(graphID);
      if (contained) {
        break;
      }
    }

    return contained;
  }
}
