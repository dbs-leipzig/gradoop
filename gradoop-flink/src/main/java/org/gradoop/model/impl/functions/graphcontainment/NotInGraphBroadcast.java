package org.gradoop.model.impl.functions.graphcontainment;

import org.gradoop.model.api.EPGMGraphElement;

/**
 * True, if an element is not contained in a given graph.
 *
 * @param <GE> element type
 */
public class NotInGraphBroadcast<GE extends EPGMGraphElement>
  extends GraphContainmentFilterBroadcast<GE> {

  @Override
  public boolean filter(GE element) throws Exception {
    return !element.getGraphIds().contains(graphId);
  }
}
