package org.gradoop.model.impl.operators.equality.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;

/**
 * Maps a Vertex to a single data label (map) or a set of data labels with
 * one for each graph the vertex is contained in (flatmap).
 *
 * @param <V> vertex type
 */
public class VertexDataLabeler<V extends EPGMVertex>
  extends ElementBaseLabeler
  implements MapFunction<V, DataLabel>, FlatMapFunction<V, DataLabel> {

  @Override
  public DataLabel map(V vertex) throws Exception {
    return initDataLabel(vertex);
  }

  @Override
  public void flatMap(
    V vertex, Collector<DataLabel> collector) throws Exception {

    DataLabel dataLabel = initDataLabel(vertex);

    for (GradoopId graphId : vertex.getGraphIds()) {
      dataLabel.setGraphId(graphId);
      collector.collect(dataLabel);
    }
  }

  /**
   * DRY
   *
   * @param vertex vertex
   * @return data label
   */
  private DataLabel initDataLabel(V vertex) {
    String canonicalLabel = vertex.getLabel() + label(vertex.getProperties());

    return new DataLabel(vertex.getId(), canonicalLabel);
  }
}
