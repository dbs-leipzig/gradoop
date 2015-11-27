package org.gradoop.model.impl.functions.graphcontainment;

import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 26.11.15.
 */
public class NotInGraphsBroadcast
  <GE extends EPGMGraphElement>
  extends GraphsContainmentFilterBroadcast<GE> {

  @Override
  public boolean filter(GE element) throws Exception {

    boolean contained = false;

    for(GradoopId graphID : graphIds) {
      contained = element.getGraphIds().contains(graphID);
      if(contained) {
        break;
      }
    }

    return !contained;
  }
}
