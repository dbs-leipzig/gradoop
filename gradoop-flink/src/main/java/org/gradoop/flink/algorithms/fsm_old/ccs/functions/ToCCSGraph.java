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

package org.gradoop.flink.algorithms.fsm_old.ccs.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm_old.ccs.CategoryCharacteristicSubgraphs;
import org.gradoop.flink.algorithms.fsm_old.ccs.pojos.CCSGraph;
import org.gradoop.flink.algorithms.fsm_old.common.functions.ToFSMGraph;
import org.gradoop.flink.algorithms.fsm_old.common.pojos.FSMEdge;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;

import java.util.Map;

/**
 * graphTransaction => categorizedGraph
 */
public class ToCCSGraph extends ToFSMGraph
  implements MapFunction<GraphTransaction, CCSGraph> {

  @Override
  public CCSGraph map(GraphTransaction graph) throws Exception {

    String category = graph.getGraphHead().getPropertyValue(
      CategoryCharacteristicSubgraphs.CATEGORY_KEY).toString();

    Map<GradoopId, Integer> vertexIdMap =
      Maps.newHashMapWithExpectedSize(graph.getVertices().size());

    Map<Integer, String> fsmVertices = transformVertices(graph, vertexIdMap);

    Map<Integer, FSMEdge> fsmEdges = transformEdges(graph, vertexIdMap);

    return new CCSGraph(
      category, graph.getGraphHead().getId(), fsmVertices, fsmEdges);
  }
}
