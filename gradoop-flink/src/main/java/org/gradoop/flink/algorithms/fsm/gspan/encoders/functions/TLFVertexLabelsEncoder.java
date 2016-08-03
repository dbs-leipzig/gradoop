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

package org.gradoop.flink.algorithms.fsm.gspan.encoders.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.tuples.EdgeTripleWithStringEdgeLabel;


import org.gradoop.flink.io.impl.tlf.tuples.TLFEdge;
import org.gradoop.flink.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.flink.io.impl.tlf.tuples.TLFVertex;
import org.gradoop.flink.algorithms.fsm.config.BroadcastNames;

import java.util.Collection;
import java.util.Map;

/**
 * G => {e,..}
 * edges in gSpan specific representation;
 * tlf vertex labels are translated from string to integer
 *
 */
public class TLFVertexLabelsEncoder
  extends RichMapFunction
    <TLFGraph, Collection<EdgeTripleWithStringEdgeLabel<Integer>>> {

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
  public Collection<EdgeTripleWithStringEdgeLabel<Integer>> map(
    TLFGraph tlfGraph) throws Exception {

    Map<Integer, Integer> vertexLabels = Maps.newHashMap();
    Collection<EdgeTripleWithStringEdgeLabel<Integer>> triples =
      Lists.newArrayList();

    for (TLFVertex vertex : tlfGraph.getGraphVertices()) {
      Integer label = dictionary.get(vertex.getLabel());

      if (label != null) {
        vertexLabels.put(vertex.getId(), label);
      }
    }

    for (TLFEdge edge : tlfGraph.getGraphEdges()) {
      Integer sourceLabel = vertexLabels.get(edge.getSourceId());
      if (sourceLabel != null) {
        Integer targetLabel = vertexLabels.get(edge.getTargetId());
        if (targetLabel != null) {
          triples.add(new EdgeTripleWithStringEdgeLabel<Integer>(
            edge.getSourceId(),
            edge.getTargetId(),
            edge.getLabel(),
            sourceLabel,
            targetLabel
          ));
        }
      }
    }
    return triples;
  }
}
