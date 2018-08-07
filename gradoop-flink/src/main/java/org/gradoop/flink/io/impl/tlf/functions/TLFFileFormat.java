/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.tlf.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.tlf.TLFConstants;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.Map;
import java.util.Set;

/**
 * Converts a GraphTransaction to the following format:
 * <p>
 *   t # 0
 *   v 0 vertexLabel0
 *   v 1 vertexLabel1
 *   e 0 1 edgeLabel
 * </p>
 */
public class TLFFileFormat
  implements TextOutputFormat.TextFormatter<GraphTransaction> {

  /**
   * Global counter for the graph id used for each single graph transaction.
   */
  private long graphId = 0;

  /**
   * Stores a long representation for each gradoop id.
   */
  private Map<GradoopId, Long> vertexIdMap;

  /**
   * Creates a TLF string representation of a given graph transaction, which
   * has the following format:
   * <p>
   * t # 0
   * v 0 label
   * v 1 label
   * v 2 label
   * e 0 1 edgeLabel
   * e 1 2 edgeLabel
   * e 2 1 edgeLabel
   * </p>
   *
   * @param graphTransaction graph transaction
   * @return TLF string representation
   */
  @Override
  public String format(GraphTransaction graphTransaction) {
    StringBuilder builder = new StringBuilder();

    vertexIdMap = Maps
      .newHashMapWithExpectedSize(graphTransaction.getVertices().size());

    // GRAPH HEAD
    writeGraphHead(builder, graphId);
    graphId++;

    // VERTICES
    writeVertices(builder, graphTransaction.getVertices());

    // EDGES
    writeEdges(builder, graphTransaction.getEdges());

    return builder.toString().trim();
  }

  /**
   * Converts the graph head into the following format and adds this to the
   * StringBuilder:
   * <p>
   * t # 0
   * </p>
   *
   * @param builder StringBuilder to build the whole string representation
   * @param graphId current graph id
   * @return builder with appended graph head representation
   */
  private StringBuilder writeGraphHead(StringBuilder builder, long graphId) {
    return builder.append(String.format("%s %s %s%n",
      TLFConstants.GRAPH_SYMBOL, TLFConstants.NEW_GRAPH_TAG, graphId));
  }

  /**
   * Converts the vertices into the following format and adds this to the
   * StringBuilder:
   * <p>
   * v 0 vertexLabel
   * v 1 vertexLabel
   * v 2 vertexLabel
   * </p>
   *
   * @param builder StringBuilder to build the whole string representation
   * @param vertices set of vertices
   * @return builder with appended vertex representations
   */
  private StringBuilder writeVertices(StringBuilder builder,
    Set<Vertex> vertices) {
    long vertexId = 0;
    for (Vertex vertex : vertices) {
      vertexIdMap.put(vertex.getId(), vertexId);
      builder.append(String.format("%s %s %s%n",
        TLFConstants.VERTEX_SYMBOL,
        vertexId,
        vertex.getLabel()));
      vertexId++;
    }
    return builder;
  }

  /**
   * Converts the edges into the following format and adds this to the
   * StringBuilder:
   * <p>
   * e 0 1 edgeLabel
   * e 1 2 edgeLabel
   * e 2 1 edgeLabel
   * </p>
   *
   * @param builder StringBuilder to build the whole string representation
   * @param edges set of edges
   * @return builder with appended edge representations
   */
  private StringBuilder writeEdges(StringBuilder builder, Set<Edge> edges) {
    for (Edge edge : edges) {
      Long sourceId = vertexIdMap.get(edge.getSourceId());
      Long targetId = vertexIdMap.get(edge.getTargetId());

      builder.append(String.format("%s %s %s %s%n",
        TLFConstants.EDGE_SYMBOL,
        sourceId,
        targetId,
        edge.getLabel()));
    }
    return builder;
  }
}
