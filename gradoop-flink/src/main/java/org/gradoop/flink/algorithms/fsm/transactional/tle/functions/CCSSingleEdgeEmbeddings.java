
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.CCSGraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraphEmbeddings;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;

import java.util.List;
import java.util.Map;

/**
 * graph => embedding(k=1),..
 */
public class CCSSingleEdgeEmbeddings extends SingleEdgeEmbeddings
  implements FlatMapFunction<CCSGraph, CCSSubgraphEmbeddings> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final CCSSubgraphEmbeddings reuseTuple = new CCSSubgraphEmbeddings();

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public CCSSingleEdgeEmbeddings(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  public void flatMap(CCSGraph graph,
    Collector<CCSSubgraphEmbeddings> out) throws Exception {

    Map<String, List<Embedding>> subgraphEmbeddings =
      createEmbeddings(graph);

    reuseTuple.setCategory(graph.getCategory());
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
