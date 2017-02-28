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

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Map;

/**
 * Gradoop Graph Transaction => lightweight labeled graph
 */
public class EPGMGraphTransactionToLabeledGraph implements
  MapFunction<GraphTransaction, LabeledGraphStringString> {

  @Override
  public LabeledGraphStringString map(GraphTransaction transaction) throws Exception {

    LabeledGraphStringString outGraph = LabeledGraphStringString.getEmptyOne();

    Map<String, Integer> labelMap = Maps.newHashMap();
    Map<GradoopId, Integer> vertexIdMap = Maps.newHashMap();

    for (Vertex vertex : transaction.getVertices()) {
      vertexIdMap.put(vertex.getId(), outGraph.addVertex(vertex.getLabel()));
    }

    labelMap.clear();

    for (Edge edge : transaction.getEdges()) {
      int sourceId = vertexIdMap.get(edge.getSourceId());
      int targetId = vertexIdMap.get(edge.getTargetId());
      outGraph.addEdge(sourceId, edge.getLabel(), targetId);
    }

    return outGraph;
  }
}
