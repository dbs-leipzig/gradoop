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

package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.common.functions.ToFSMGraph;
import org.gradoop.flink.algorithms.fsm.common.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.tfsm.pojos.TFSMGraph;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.TFSMGraph;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;

import java.util.Map;

/**
 * graphTransaction => fsmGraph
 */
public class ToTFSMGraph extends ToFSMGraph
  implements MapFunction<GraphTransaction, TFSMGraph> {

  @Override
  public TFSMGraph map(GraphTransaction graph) throws Exception {

    Map<GradoopId, Integer> vertexIdMap =
      Maps.newHashMapWithExpectedSize(graph.getVertices().size());

    Map<Integer, String> fsmVertices = transformVertices(graph, vertexIdMap);

    Map<Integer, FSMEdge> fsmEdges = transformEdges(graph, vertexIdMap);

    return new TFSMGraph(graph.getGraphHead().getId(), fsmVertices, fsmEdges);
  }
}
