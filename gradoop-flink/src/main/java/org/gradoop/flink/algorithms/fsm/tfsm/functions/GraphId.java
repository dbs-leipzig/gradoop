package org.gradoop.flink.algorithms.fsm.tfsm.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.tfsm.pojos.TFSMGraph;

public class GraphId implements KeySelector<TFSMGraph, GradoopId> {
  @Override
  public GradoopId getKey(TFSMGraph graph) throws Exception {
    return graph.getId();
  }
}
