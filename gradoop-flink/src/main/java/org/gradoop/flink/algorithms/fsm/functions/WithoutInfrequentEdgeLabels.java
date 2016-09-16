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

package org.gradoop.flink.algorithms.fsm.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.config.Constants;
import org.gradoop.flink.algorithms.fsm.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.tuples.FSMGraph;

import java.util.Collection;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Drops all edges with infrequent labels and afterwards isolated vertices.
 */
public class WithoutInfrequentEdgeLabels
  extends RichMapFunction<FSMGraph, FSMGraph> {

  /**
   * frequent edge labels submitted via broadcast
   */
  private Collection<String> frequentEdgeLabels;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.frequentEdgeLabels = getRuntimeContext()
      .getBroadcastVariable(Constants.FREQUENT_EDGE_LABELS);

    this.frequentEdgeLabels = Sets.newHashSet(frequentEdgeLabels);
  }

  @Override
  public FSMGraph map(FSMGraph value) throws Exception {

    Set<Integer> connectedVertexIds = Sets.newHashSet();

    Iterator<Map.Entry<Integer, FSMEdge>> edgeIterator =
      value.getEdges().entrySet().iterator();

    while (edgeIterator.hasNext()) {
      FSMEdge edge = edgeIterator.next().getValue();

      if (frequentEdgeLabels.contains(edge.getLabel())) {
        connectedVertexIds.add(edge.getSourceId());
        connectedVertexIds.add(edge.getTargetId());
      } else {
        edgeIterator.remove();
      }
    }

    Iterator<Map.Entry<Integer, String>> vertexIterator =
      value.getVertices().entrySet().iterator();

    while (vertexIterator.hasNext()) {
      Map.Entry<Integer, String> vertex = vertexIterator.next();

      if (! connectedVertexIds.contains(vertex.getKey())) {
        vertexIterator.remove();
      }
    }

    return value;
  }
}
