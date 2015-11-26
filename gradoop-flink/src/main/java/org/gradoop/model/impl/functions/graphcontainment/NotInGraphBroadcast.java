package org.gradoop.model.impl.functions.graphcontainment;

import org.gradoop.model.api.EPGMGraphElement;

public class NotInGraphBroadcast<GE extends EPGMGraphElement>
  extends AbstractBroadcastGraphContainmentFilter<GE> {

  @Override
  public boolean filter(GE element) throws Exception {
    return element.getGraphIds().contains(graphId);
  }
}
