package org.gradoop.model.impl.operators.matching.common.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;

/**
 * Represents a source vertex, edge and target vertex triple that matches at
 * least one triple in the database. Each triple contains a list of identifiers
 * that match to edge ids in the query graph.

 * f0: edge id
 * f1: source vertex id
 * f2: target vertex id
 * f3: query candidates
 */
public class MatchingTriple
  extends Tuple4<GradoopId, GradoopId, GradoopId, List<Long>> {

  public GradoopId getEdgeId() {
    return f0;
  }

  public void setEdgeId(GradoopId id) {
    f0 = id;
  }

  public GradoopId getSourceVertexId() {
    return f1;
  }

  public void setSourceVertexId(GradoopId id) {
    f1 = id;
  }

  public GradoopId getTargetVertexId() {
    return f2;
  }

  public void setTargetVertexId(GradoopId id) {
    f2 = id;
  }

  public List<Long> getQueryCandidates() {
    return f3;
  }

  public void setQueryCandidates(List<Long> ids) {
    f3 = ids;
  }
}
