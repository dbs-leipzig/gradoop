package org.gradoop.model.impl.functions.graphcontainment;

import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;

/**
 * True, if an element is not contained in any of a given set of graphs.
 *
 * @param <GE> element type
 */
public class NotInGraphsBroadcast<GE extends EPGMGraphElement>
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

    return !contained;
  }
}
