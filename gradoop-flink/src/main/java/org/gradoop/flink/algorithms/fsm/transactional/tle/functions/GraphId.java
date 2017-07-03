
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMGraph;

/**
 * graph => id
 *
 * @param <G> graph type
 */
public class GraphId<G extends FSMGraph> implements KeySelector<G, GradoopId> {

  @Override
  public GradoopId getKey(G graph) throws Exception {
    return graph.getId();
  }
}
