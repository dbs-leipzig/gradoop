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

package org.gradoop.flink.model.impl.operators.matching.transactional.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a graph with the candidates of its elements.
 * <p>
 * f0: graph id
 * f1: list of vertex ids and the candidates for this vertex
 * f2: list of edge ids and the candidates for this edge
 */
public class GraphWithCandidates extends
  Tuple3<GradoopId, List<IdWithCandidates<GradoopId>>, List<TripleWithCandidates<GradoopId>>> {

  /**
   * Default Constructor
   */
  public GraphWithCandidates() {

  }

  /**
   * Constructor
   * @param id graph id
   */
  public GraphWithCandidates(GradoopId id) {
    this.setGraphId(id);
    this.setVertexCandidates(new ArrayList<>());
    this.setEdgeCandidates(new ArrayList<>());
  }

  public void setGraphId(GradoopId id) {
    this.f0 = id;
  }

  public GradoopId getGraphId() {
    return this.f0;
  }

  public void setVertexCandidates(List<IdWithCandidates<GradoopId>> vertexCandidates) {
    this.f1 = vertexCandidates;
  }

  public List<IdWithCandidates<GradoopId>> getVertexCandidates() {
    return this.f1;
  }

  public void setEdgeCandidates(List<TripleWithCandidates<GradoopId>> edgeCandidates) {
    this.f2 = edgeCandidates;
  }

  public List<TripleWithCandidates<GradoopId>> getEdgeCandidates() {
    return this.f2;
  }
}
