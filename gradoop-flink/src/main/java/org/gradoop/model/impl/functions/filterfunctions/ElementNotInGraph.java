package org.gradoop.model.impl.functions.filterfunctions;

import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.api.EPGMGraphHead;

/**
 * Created by peet on 26.11.15.
 */
public class ElementNotInGraph
  <G extends EPGMGraphHead, GE extends EPGMGraphElement>
  extends AbstractGraphContainmentFilter<G, GE> {

  @Override
  public boolean filter(GE element) throws Exception {
    return element.getGraphIds().contains(graphHead.getId());
  }
}
