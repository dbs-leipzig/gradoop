
package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Set;

/**
 * transaction -> (vertexLabel,1L),..
 */
public class VertexLabels
  implements FlatMapFunction<GraphTransaction, WithCount<String>> {

  /**
   * reuse tuple to avoid instantiations
   */
  private WithCount<String> reuseTuple = new WithCount<>(null, 1);

  @Override
  public void flatMap(GraphTransaction graph,
    Collector<WithCount<String>> out) throws Exception {

    Set<String> vertexLabels = Sets.newHashSet();

    for (Vertex vertex : graph.getVertices()) {
      vertexLabels.add(vertex.getLabel());
    }

    for (String label : vertexLabels) {
      reuseTuple.setObject(label);
      out.collect(reuseTuple);
    }
  }
}
