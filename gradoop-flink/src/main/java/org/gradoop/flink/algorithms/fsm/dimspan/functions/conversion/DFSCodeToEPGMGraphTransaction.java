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

package org.gradoop.flink.algorithms.fsm.dimspan.functions.conversion;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtilsBase;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Set;

/**
 * int-array encoded graph => Gradoop Graph Transaction
 */
public class DFSCodeToEPGMGraphTransaction extends RichMapFunction<int[], GraphTransaction> {

  /**
   * frequent vertex labels
   */
  private String[] vertexDictionary;

  /**
   * frequent vertex labels
   */
  private String[] edgeDictionary;

  /**
   * utils to interpret and manipulate integer encoded graphs
   */
  private final GraphUtils graphUtils = new GraphUtilsBase();

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    vertexDictionary = getRuntimeContext()
      .<String[]>getBroadcastVariable(DIMSpanConstants.VERTEX_DICTIONARY).get(0);

    edgeDictionary = getRuntimeContext()
      .<String[]>getBroadcastVariable(DIMSpanConstants.EDGE_DICTIONARY).get(0);

  }

  @Override
  public GraphTransaction map(int[] inGraph) throws Exception {

    // GRAPH HEAD
    GraphHead graphHead = new GraphHead(GradoopId.get(), "", null);
    GradoopIdList graphIds = GradoopIdList.fromExisting(graphHead.getId());

    // VERTICES
    int[] vertexLabels = graphUtils.getVertexLabels(inGraph);

    GradoopId[] vertexIds = new GradoopId[vertexLabels.length];

    Set<Vertex> vertices = Sets.newHashSetWithExpectedSize(vertexLabels.length);

    int intId = 0;
    for (int intLabel : vertexLabels) {
      String label = vertexDictionary[intLabel];

      GradoopId gradoopId = GradoopId.get();
      vertices.add(new Vertex(gradoopId, label, null, graphIds));

      vertexIds[intId] = gradoopId;
      intId++;
    }

    // EDGES
    Set<Edge> edges = Sets.newHashSet();

    for (int edgeId = 0; edgeId < graphUtils.getEdgeCount(inGraph); edgeId++) {
      String label = edgeDictionary[graphUtils.getEdgeLabel(inGraph, edgeId)];

      GradoopId sourceId;
      GradoopId targetId;

      if (graphUtils.isOutgoing(inGraph, edgeId)) {
        sourceId = vertexIds[graphUtils.getFromId(inGraph, edgeId)];
        targetId = vertexIds[graphUtils.getToId(inGraph, edgeId)];
      } else {
        sourceId = vertexIds[graphUtils.getToId(inGraph, edgeId)];
        targetId = vertexIds[graphUtils.getFromId(inGraph, edgeId)];
      }

      edges.add(new Edge(GradoopId.get(), label, sourceId, targetId, null, graphIds));
    }

    return new GraphTransaction(graphHead, vertices, edges);
  }
}
