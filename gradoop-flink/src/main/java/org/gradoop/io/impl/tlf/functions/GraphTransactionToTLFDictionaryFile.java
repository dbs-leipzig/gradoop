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

import org.apache.flink.api.java.io.TextOutputFormat;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.tuples.GraphTransaction;

/**
 * Creates a dictionary format from a GraphTransaction as followed:
 * <p>
 *   label0 0
 *   label1 1
 *   ...
 * </p>
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GraphTransactionToTLFDictionaryFile<G extends
  EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> implements
  TextOutputFormat.TextFormatter<GraphTransaction<G, V, E>> {

  /**
   * '1' if write a vertex dictionary, '-1' if write an edge dictionary.
   */
  private int dictionaryType;

  /**
   * Creates a new text output format for either a vertex or an edge dictionary.
   *
   * @param dictionaryType set to '1' for writing a vertex dictionary, and
   *                         '-1' for writing an edge dictionary
   */
  public GraphTransactionToTLFDictionaryFile(int dictionaryType) {
    this.dictionaryType = dictionaryType;
  }

  /**
   * Creates a TLF dictionary string representation of a given graph
   * transaction.
   *
   * @param graphTransaction graph transaction
   * @return TLF dictionary string representation
   */
  @Override
  public String format(GraphTransaction<G, V, E> graphTransaction) {
    StringBuilder dictionary = new StringBuilder();

    String[] label;
    if (dictionaryType == TLFDictionaryConstants.VERTEX_DICTIONARY) {
      for (V vertex: graphTransaction.getVertices()) {
        label = vertex.getLabel().split(TLFDictionaryConstants.LABEL_SPLIT);
        dictionary.append(label[0] + " " + label[1] + "\n");
      }
    } else if (dictionaryType == TLFDictionaryConstants.EDGE_DICTIONARY){
      for (E edge: graphTransaction.getEdges()) {
        label = edge.getLabel().split(TLFDictionaryConstants.LABEL_SPLIT);
        dictionary.append(label[0] + " " + label[1] + "\n");
      }
    }
    return dictionary.toString();
  }
}
