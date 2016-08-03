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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.tlf.tuples.TLFEdge;
import org.gradoop.flink.io.impl.tlf.tuples.TLFVertex;
import org.gradoop.flink.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Collection;
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
    Map<GradoopId, Integer> vertexIdMap = Maps
      .newHashMapWithExpectedSize(graphTransaction.getVertices().size());

    Collection<String> lines = Lists.newArrayListWithExpectedSize(
      graphTransaction.getVertices().size() +
        graphTransaction.getEdges().size() + 1
    );

    // GRAPH HEAD
    lines.add(TLFGraph.SYMBOL + " # " + graphId);
    graphId++;

    // VERTICES
    int vertexId = 0;
    for (Vertex vertex : graphTransaction.getVertices()) {
      vertexIdMap.put(vertex.getId(), vertexId);
      lines.add(TLFVertex.SYMBOL + " " + vertexId + " " + vertex.getLabel());
      vertexId++;
    }

    // EDGES
    for (Edge edge : graphTransaction.getEdges()) {
      Integer sourceId = vertexIdMap.get(edge.getSourceId());
      Integer targetId = vertexIdMap.get(edge.getTargetId());

      lines.add(TLFEdge.SYMBOL +
        " " + sourceId + " " + targetId + "" +  " " + edge.getLabel());
    }
    return StringUtils.join(lines, "\n");
  }
}
