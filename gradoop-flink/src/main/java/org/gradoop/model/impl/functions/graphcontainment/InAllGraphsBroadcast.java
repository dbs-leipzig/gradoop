package org.gradoop.model.impl.functions.graphcontainment;

import org.gradoop.model.api.EPGMGraphElement;

/**
 * True, if an element is contained in all of a set of given graphs.
 *
 * @param <GE> element type
 */
public class InAllGraphsBroadcast
  <GE extends EPGMGraphElement>
  extends GraphsContainmentFilterBroadcast<GE> {

  @Override
  public boolean filter(GE element) throws Exception {

    return element.getGraphIds().containsAll(this.graphIds);
  }
}
