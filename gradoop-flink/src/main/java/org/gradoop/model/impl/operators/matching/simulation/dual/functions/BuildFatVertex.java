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

package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichGroupCombineFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.FatVertex;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.IdPair;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithDirection;

import java.util.List;

/**
 * Combines a collection of {@link TripleWithDirection} to a {@link FatVertex}.
 */
public class BuildFatVertex
  extends RichGroupCombineFunction<TripleWithDirection, FatVertex> {

  private final FatVertex reuseVertex = new FatVertex();

  private final String query;

  private QueryHandler queryHandler;

  public BuildFatVertex(String query) {
    this.query = query;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.queryHandler = QueryHandler.fromString(query);
  }

  @Override
  public void combine(Iterable<TripleWithDirection> triples,
    Collector<FatVertex> collector) throws Exception {

    boolean first = true;

    for (TripleWithDirection triple : triples) {
      if (first) {
        initFatVertex(triple);
        first = false;
      }

      if (triple.isOutgoing()) {
        processOutgoingEdgeTriple(triple);
      } else {
        processIncomingEdgeTriple(triple);
      }
    }
    collector.collect(reuseVertex);
  }

  private void initFatVertex(TripleWithDirection tripleWithDirection) {
    reuseVertex.setVertexId(tripleWithDirection.getSourceId());
    reuseVertex.setCandidates(Lists.<Long>newArrayList());
    reuseVertex.setParentIds(Lists.<GradoopId>newArrayList());
    reuseVertex.setIncomingCandidateCounts(new int[queryHandler.getEdgeCount()]);
    reuseVertex.setEdgeCandidates(Maps.<IdPair, List<Long>>newHashMap());
    reuseVertex.isUpdated(true);
  }

  private void processOutgoingEdgeTriple(TripleWithDirection triple) {
    for (Long queryEdgeId : triple.getCandidates()) {
      // update CA
      updateCandidates(queryHandler.getEdgeById(queryEdgeId).getSourceVertexId());
    }
    // update OUT_CA
    updateOutgoingEdges(triple);
  }

  private void updateOutgoingEdges(TripleWithDirection triple) {
    IdPair idPair = new IdPair();
    idPair.setEdgeId(triple.getEdgeId());
    idPair.setTargetId(triple.getTargetId());
    reuseVertex.getEdgeCandidates().put(idPair, triple.getCandidates());
  }

  private void processIncomingEdgeTriple(TripleWithDirection triple) {
    for (Long queryEdgeId : triple.getCandidates()) {
      // update IN_CA
      reuseVertex.getIncomingCandidateCounts()[queryEdgeId.intValue()]++;
      // update P_IDs
      updateParentIds(triple);
      // update CA
      updateCandidates(queryHandler.getEdgeById(queryEdgeId).getTargetVertexId());
    }
  }

  private void updateCandidates(Long candidate) {
    if (!reuseVertex.getCandidates().contains(candidate)) {
      reuseVertex.getCandidates().add(candidate);
    }
  }

  private void updateParentIds(TripleWithDirection triple) {
    if (!reuseVertex.getParentIds().contains(triple.getTargetId())) {
      reuseVertex.getParentIds().add(triple.getTargetId());
    }
  }
}
