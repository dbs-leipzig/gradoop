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

package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichGroupCombineFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.FatVertex;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.IdPair;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.TripleWithDirection;

/**
 * Combines a collection of {@link TripleWithDirection} to a {@link FatVertex}.
 */
public class BuildFatVertex
  extends RichGroupCombineFunction<TripleWithDirection, FatVertex> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Reduce instantiations
   */
  private final FatVertex reuseVertex;

  /**
   * GDL query
   */
  private final String query;

  /**
   * Query handler
   */
  private transient QueryHandler qHandler;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public BuildFatVertex(String query) {
    this.query        = query;
    this.reuseVertex  = new FatVertex();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.qHandler = new QueryHandler(query);
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

  /**
   * Initializes the fat vertex.
   *
   * @param triple edge triple
   */
  private void initFatVertex(TripleWithDirection triple) {
    reuseVertex.setVertexId(triple.getSourceId());
    reuseVertex.setCandidates(Lists.newArrayList());
    reuseVertex.setParentIds(Lists.newArrayList());
    reuseVertex.setIncomingCandidateCounts(new int[qHandler.getEdgeCount()]);
    reuseVertex.setEdgeCandidates(Maps.newHashMap());
    reuseVertex.setUpdated(true);
  }

  /**
   * Updates vertex candidates and outgoing edges of the fat vertex based on the
   * given outgoing edge triple.
   *
   * @param triple outgoing edge tripe
   */
  private void processOutgoingEdgeTriple(TripleWithDirection triple) {
    for (int eQ = 0; eQ < triple.getCandidates().length; eQ++) {
      if (triple.getCandidates()[eQ]) {
        updateCandidates(qHandler.getEdgeById((long) eQ).getSourceVertexId());
      }
    }
    // update outgoing edges (OUT_CA)
    updateOutgoingEdges(triple);
  }

  /**
   * Updates query candidates of the resulting fat vertex.
   *
   * @param candidate query vertex id
   */
  private void updateCandidates(Long candidate) {
    if (!reuseVertex.getCandidates().contains(candidate)) {
      reuseVertex.getCandidates().add(candidate);
    }
  }

  /**
   * Updates outgoing edges of the resulting fat vertex.
   *
   * @param triple outgoing edge triple
   */
  private void updateOutgoingEdges(TripleWithDirection triple) {
    IdPair idPair = new IdPair();
    idPair.setEdgeId(triple.getEdgeId());
    idPair.setTargetId(triple.getTargetId());
    reuseVertex.getEdgeCandidates().put(idPair, triple.getCandidates());
  }

  /**
   * Updates vertex candidates, parent ids and incoming edge candidate counts of
   * the fat vertex based on the given incoming edge triple.
   *
   * @param triple incoming edge triple
   */
  private void processIncomingEdgeTriple(TripleWithDirection triple) {
    for (int eQ = 0; eQ < triple.getCandidates().length; eQ++) {
      if (triple.getCandidates()[eQ]) {
        // update incoming edge counts (IN_CA)
        reuseVertex.getIncomingCandidateCounts()[eQ]++;
        // update parent ids (P_IDs)
        updateParentIds(triple);
        // update vertex candidates (CA)
        updateCandidates(qHandler.getEdgeById((long) eQ).getTargetVertexId());
      }
    }
  }

  /**
   * Adds the targetId of the given triple to the parent ids.
   *
   * @param triple incoming edge triple
   */
  private void updateParentIds(TripleWithDirection triple) {
    if (!reuseVertex.getParentIds().contains(triple.getTargetId())) {
      reuseVertex.getParentIds().add(triple.getTargetId());
    }
  }
}
