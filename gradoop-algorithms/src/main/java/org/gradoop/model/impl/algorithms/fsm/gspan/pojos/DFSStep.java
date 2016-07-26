package org.gradoop.model.impl.algorithms.fsm.gspan.pojos;

import java.io.Serializable;

/**
 * Created by peet on 26.07.16.
 */
public interface DFSStep extends Serializable {
  int getFromTime();

  Integer getFromLabel();

  Boolean isOutgoing();

  Integer getEdgeLabel();

  int getToTime();

  Integer getToLabel();

  Boolean isLoop();

  Boolean isForward();

  Boolean isBackward();

  int getMinVertexLabel();
}
