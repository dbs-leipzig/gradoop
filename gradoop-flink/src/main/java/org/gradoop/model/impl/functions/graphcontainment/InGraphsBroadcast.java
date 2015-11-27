package org.gradoop.model.impl.functions.graphcontainment;

import org.gradoop.model.api.EPGMGraphElement;

/**
 * Created by peet on 26.11.15.
 */
public class InGraphsBroadcast
  <GE extends EPGMGraphElement>
  extends GraphsContainmentFilterBroadcast<GE> {

  @Override
  public boolean filter(GE element) throws Exception {

    return element.getGraphIds().containsAll(this.graphIds);
  }
}
