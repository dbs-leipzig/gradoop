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

package org.gradoop.io.impl.tlf.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps the vertex dictionary to a given graph transaction. The
 * integer-like labels are replaced by those from the dictionary files
 * where the integer value from the old labels matches the corresponding
 * keys from the dictionary.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class TLFDictionaryVertexLabelToTransaction<G extends EPGMGraphHead,
  V extends EPGMVertex, E extends EPGMEdge> extends
  RichMapFunction<GraphTransaction<G, V, E>, GraphTransaction<G, V, E>> {
  /**
   * Constant for broadcast set containing the vertex dictionary.
   */
  public static final String VERTEX_DICTIONARY = "vertexDictionary";
  /**
   * Constant string which is added to those edges or vertices which do not
   * have an entry in the dictionary while others have one.
   */
  private static final String NO_LABEL = " - no label";
  /**
   * Map which contains a vertex dictionary.
   */
  private Map<Integer, String> vertexDictionary;

  /**
   * {@inheritDoc}
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    vertexDictionary = getRuntimeContext()
      .<HashMap<Integer, String>>getBroadcastVariable(VERTEX_DICTIONARY)
      .get(0);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphTransaction<G, V, E> map(
    GraphTransaction<G, V, E> graphTransaction) throws Exception {
    String label;
    for (V vertex : graphTransaction.getVertices()) {
      label = vertexDictionary.get(Integer.parseInt(vertex.getLabel()));
      if (label != null) {
        vertex.setLabel(label);
      } else {
        vertex.setLabel(vertex.getLabel() + NO_LABEL);
      }
    }
    return graphTransaction;
  }
}
