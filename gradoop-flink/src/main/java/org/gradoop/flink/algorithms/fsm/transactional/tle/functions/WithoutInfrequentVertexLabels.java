
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMGraph;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Drops all vertices with infrequent labels and edges connecting only such
 * vertices.
 *
 * @param <G> graph type
 */
public class WithoutInfrequentVertexLabels<G extends FSMGraph>
  extends RichMapFunction<G, G> {

  /**
   * frequent vertex labels submitted via broadcast
   */
  private Collection<String> frequentVertexLabels;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.frequentVertexLabels = getRuntimeContext()
      .getBroadcastVariable(TFSMConstants.FREQUENT_VERTEX_LABELS);

    this.frequentVertexLabels = Sets.newHashSet(frequentVertexLabels);
  }

  @Override
  public G map(G value) throws Exception {

    Set<Integer> keptVertexIds = Sets.newHashSet();

    Iterator<Map.Entry<Integer, String>> vertexIterator =
      value.getVertices().entrySet().iterator();

    while (vertexIterator.hasNext()) {
      Map.Entry<Integer, String> vertex = vertexIterator.next();

      if (frequentVertexLabels.contains(vertex.getValue())) {
        keptVertexIds.add(vertex.getKey());
      } else {
        vertexIterator.remove();
      }
    }

    Iterator<Map.Entry<Integer, FSMEdge>> edgeIterator =
      value.getEdges().entrySet().iterator();

    while (edgeIterator.hasNext()) {
      FSMEdge edge = edgeIterator.next().getValue();

      if (! (keptVertexIds.contains(edge.getSourceId()) &&
        keptVertexIds.contains(edge.getTargetId()))) {
        edgeIterator.remove();
      }
    }

    return value;
  }
}
