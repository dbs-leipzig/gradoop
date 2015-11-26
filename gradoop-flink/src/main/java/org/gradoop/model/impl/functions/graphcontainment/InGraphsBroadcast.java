package org.gradoop.model.impl.functions.graphcontainment;

import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 26.11.15.
 */
public class InGraphsBroadcast
  <GE extends EPGMGraphElement>
  extends AbstractBroadcastGraphsContainmentFilter<GE> {

  @Override
  public boolean filter(GE element) throws Exception {

    return element.getGraphIds().containsAll(this.graphIds);
  }
}
