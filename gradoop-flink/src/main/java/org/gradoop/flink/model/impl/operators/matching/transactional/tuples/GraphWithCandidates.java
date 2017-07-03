
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
