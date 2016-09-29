package org.gradoop.flink.algorithms.fsm.tfsm.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.tfsm.tuples.TFSMSubgraph;
import org.gradoop.flink.algorithms.fsm.tfsm.tuples.TFSMSubgraphEmbeddings;

public class FrequentSubgraphOrCollector implements FlatJoinFunction
  <TFSMSubgraphEmbeddings, TFSMSubgraph, TFSMSubgraphEmbeddings> {

  @Override
  public void join(TFSMSubgraphEmbeddings embeddings, TFSMSubgraph tfsmSubgraph,
    Collector<TFSMSubgraphEmbeddings> collector) throws Exception {

    if (tfsmSubgraph != null ||
      embeddings.getGraphId().equals(GradoopId.NULL_VALUE)) {
        collector.collect(embeddings);
    }
  }
}
