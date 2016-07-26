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

package org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.algorithms.fsm.config.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples.EdgeTripleWithStringEdgeLabel;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.Collection;
import java.util.Map;

/**
 * G => {e,..}
 * edges in gSpan specific representation;
 * vertex labels are translated from string to integer
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class VertexLabelsEncoder
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends RichMapFunction<GraphTransaction<G, V, E>,
    Collection<EdgeTripleWithStringEdgeLabel<GradoopId>>> {

  /**
   * vertex label dictionary
   */
  private Map<String, Integer> dictionary;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.dictionary = getRuntimeContext().<Map<String, Integer>>
      getBroadcastVariable(BroadcastNames.VERTEX_DICTIONARY).get(0);
  }


  @Override
  public Collection<EdgeTripleWithStringEdgeLabel<GradoopId>> map(
    GraphTransaction<G, V, E> transaction) throws Exception {

    Map<GradoopId, Integer> vertexLabels = Maps.newHashMap();
    Collection<EdgeTripleWithStringEdgeLabel<GradoopId>> triples =
      Lists.newArrayList();

    for (V vertex : transaction.getVertices()) {
      Integer label = dictionary.get(vertex.getLabel());

      if (label != null) {
        vertexLabels.put(vertex.getId(), label);
      }
    }

    for (E edge : transaction.getEdges()) {

      Integer sourceLabel = vertexLabels.get(edge.getSourceId());

      if (sourceLabel != null) {
        Integer targetLabel = vertexLabels.get(edge.getTargetId());

        if (targetLabel != null) {
          triples.add(new EdgeTripleWithStringEdgeLabel<>(
            edge.getSourceId(),
            edge.getTargetId(),
            edge.getLabel(),
            sourceLabel,
            targetLabel));
        }
      }
    }

    return triples;
  }
}
