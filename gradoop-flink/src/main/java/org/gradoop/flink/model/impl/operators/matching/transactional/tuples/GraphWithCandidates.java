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
  public GraphWithCandidates() { }

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
