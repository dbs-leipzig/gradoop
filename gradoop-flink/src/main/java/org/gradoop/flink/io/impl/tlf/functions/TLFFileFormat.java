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

package org.gradoop.flink.io.impl.tlf.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.tlf.tuples.TLFEdge;
import org.gradoop.flink.io.impl.tlf.tuples.TLFVertex;
import org.gradoop.flink.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Map;

/**
 * Converts a GraphTransaction to the following format:
 * <p>
 *   t # 0
 *   v 0 vertexLabel0
 *   v 1 vertexLabel1
 *   e 0 1 edgeLabel
 * </p>
 */
public class TLFFileFormat implements
  TextOutputFormat.TextFormatter<GraphTransaction> {

  /**
   * TLF graph number indicator
   */
  private static final String NEW_GRAPH_TAG = "#";
  /**
   * Global counter for the graph id used for each single graph transaction.
   */
  private int graphId = 0;

  /**
   * Creates a TLF string representation of a given graph transaction.
   *
   * @param graphTransaction graph transaction
   * @return TLF string representation
   */
  @Override
  public String format(GraphTransaction graphTransaction) {
    StringBuilder builder = new StringBuilder();

    Map<GradoopId, Integer> vertexIdMap = Maps
      .newHashMapWithExpectedSize(graphTransaction.getVertices().size());

    // GRAPH HEAD
    builder.append(String.format("%s %s %s%n",
      TLFGraph.SYMBOL,
      NEW_GRAPH_TAG,
      graphId));
    graphId++;

    // VERTICES
    int vertexId = 0;
    for (Vertex vertex : graphTransaction.getVertices()) {
      vertexIdMap.put(vertex.getId(), vertexId);
      builder.append(String.format("%s %s %s%n",
        TLFVertex.SYMBOL,
        vertexId,
        vertex.getLabel()));
      vertexId++;
    }

    // EDGES
    for (Edge edge : graphTransaction.getEdges()) {
      Integer sourceId = vertexIdMap.get(edge.getSourceId());
      Integer targetId = vertexIdMap.get(edge.getTargetId());

      builder.append(String.format("%s %s %s %s%n",
        TLFEdge.SYMBOL,
        sourceId,
        targetId,
        edge.getLabel()));
    }
    return builder.toString().trim();
  }
}
