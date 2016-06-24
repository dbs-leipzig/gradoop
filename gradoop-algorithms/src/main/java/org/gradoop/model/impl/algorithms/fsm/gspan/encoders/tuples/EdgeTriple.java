package org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples;

import org.gradoop.model.impl.id.GradoopId;

public interface EdgeTriple {
  Integer getEdgeLabel();

  GradoopId getSourceId();

  Integer getSourceLabel();

  GradoopId getTargetId();

  Integer getTargetLabel();
}
