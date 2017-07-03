
package org.gradoop.flink.io.impl.tlf.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.representation.transactional.GraphTransaction;

/**
 * Collects all vertex labels.
 */
public class VertexLabelList
  implements FlatMapFunction<GraphTransaction, String> {

  @Override
  public void flatMap(GraphTransaction graphTransaction, Collector<String> collector)
    throws Exception {
    for (Vertex vertex : graphTransaction.getVertices()) {
      collector.collect(vertex.getLabel());
    }
  }
}
