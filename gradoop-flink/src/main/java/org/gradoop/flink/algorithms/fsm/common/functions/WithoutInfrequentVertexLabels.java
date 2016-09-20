/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.algorithms.fsm.common.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.common.config.Constants;
import org.gradoop.flink.algorithms.fsm.common.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.common.pojos.FSMGraph;

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
      .getBroadcastVariable(Constants.FREQUENT_VERTEX_LABELS);

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
