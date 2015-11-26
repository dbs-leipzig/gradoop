package org.gradoop.model.impl.functions.filterfunctions;

import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.api.EPGMGraphHead;

/**
 * Created by peet on 26.11.15.
 */
public class ElementInGraphs
  <G extends EPGMGraphHead, GE extends EPGMGraphElement>
  extends AbstractGraphsContainmentFilter<G, GE> {

  @Override
  public boolean filter(GE element) throws Exception {

    boolean contained = false;

    for(G graphHead : graphHeads) {
      contained = element.getGraphIds().contains(graphHead.getId());
      if(!contained) {
        break;
      }
    }

    return contained;
  }
}
