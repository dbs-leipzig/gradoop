
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.TFSMGraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraphEmbeddings;

import java.util.List;
import java.util.Map;

/**
 * graph => embedding(k=1),..
 */
public class TFSMSingleEdgeEmbeddings extends SingleEdgeEmbeddings
  implements FlatMapFunction<TFSMGraph, TFSMSubgraphEmbeddings> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final TFSMSubgraphEmbeddings reuseTuple =
    new TFSMSubgraphEmbeddings();

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public TFSMSingleEdgeEmbeddings(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  public void flatMap(
    TFSMGraph graph, Collector<TFSMSubgraphEmbeddings> out) throws Exception {

    Map<String, List<Embedding>> subgraphEmbeddings =
      createEmbeddings(graph);

    reuseTuple.setGraphId(graph.getId());
    reuseTuple.setSize(1);

    for (Map.Entry<String, List<Embedding>> entry :
      subgraphEmbeddings.entrySet()) {

      reuseTuple.setCanonicalLabel(entry.getKey());
      reuseTuple.setEmbeddings(entry.getValue());

      out.collect(reuseTuple);
    }
  }

}
