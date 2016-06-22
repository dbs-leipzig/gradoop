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
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.HashMap;
import java.util.Map;

/**
 * Converts a GraphTransaction to the following format:
 * <p>
 *   t # 0
 *   v 0 vertexLabel0
 *   v 1 vertexLabel1
 *   e 0 1 edgeLabel
 * </p>
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GraphTransactionsToTLFFileFormat<G extends
  EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> implements
  TextOutputFormat.TextFormatter<GraphTransaction<G, V, E>> {

  /**
   * Global counter for the graph id used for each single graph transaction.
   */
  private Integer graphID = 0;

  /**
   * Creates a TLF string representation of a given graph transaction.
   *
   * @param graphTransaction graph transaction
   * @return TLF string representation
   */
  @Override
  public String format(GraphTransaction<G, V, E> graphTransaction) {
    Map<GradoopId, Integer> gradoopIdIntegerMapVertices = new HashMap<>();
    Integer id = 0;
    StringBuilder graph = new StringBuilder();

    //map GradoopIds to 'easy to read' Integer ids
    for (V vertex : graphTransaction.getVertices()) {
      gradoopIdIntegerMapVertices.put(vertex.getId(), id);
      id++;
    }

    //add head to string
    graph.append("t # " + graphID + "\n");
    graphID++;
    //add vertices to string
    for (V vertex : graphTransaction.getVertices()) {
      graph.append("v " + gradoopIdIntegerMapVertices.get(vertex.getId()) +
        " " + vertex.getLabel() + "\n");
    }
    //add edges to string
    int i = 0;
    for (E edge : graphTransaction.getEdges()) {
      graph.append("e " + gradoopIdIntegerMapVertices.get(edge.getSourceId
        ()) + " " + gradoopIdIntegerMapVertices.get(edge.getTargetId()) + "" +
        " " + edge.getLabel());
      if (i < graphTransaction.getEdges().size() - 1) {
        graph.append("\n");
      }
      i++;
    }
    return graph.toString();
  }
}
