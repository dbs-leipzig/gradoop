
package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.matching.ElementMatcher;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;

import java.util.Collection;

import static org.gradoop.common.util.GConstants.DEFAULT_EDGE_LABEL;

/**
 * Converts an EPGM edge to a {@link TripleWithCandidates} tuple.
 *
 * Forwarded fields:
 *
 * id -> f0:        edge id
 * sourceId -> f1:  source vertex id
 * targetId -> f1:  target vertex id
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFields("id->f0;sourceId->f1;targetId->f2")
public class BuildTripleWithCandidates<E extends Edge>
  extends AbstractBuilder<E, TripleWithCandidates<GradoopId>> {
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;
  /**
   * Query vertices to match against.
   */
  private transient Collection<org.s1ck.gdl.model.Edge> queryEdges;
  /**
   * Number of edges in the query graph
   */
  private int edgeCount;
  /**
   * Reduce instantiations
   */
  private final TripleWithCandidates<GradoopId> reuseTuple;
  /**
   * Constructor
   *
   * @param query GDL query
   */
  public BuildTripleWithCandidates(String query) {
    super(query);
    reuseTuple = new TripleWithCandidates<>();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryEdges = getQueryHandler().getEdges();
    edgeCount  = queryEdges.size();
  }

  @Override
  public TripleWithCandidates<GradoopId> map(E e) throws Exception {
    reuseTuple.setEdgeId(e.getId());
    reuseTuple.setSourceId(e.getSourceId());
    reuseTuple.setTargetId(e.getTargetId());
    reuseTuple.setCandidates(
      getCandidates(edgeCount, ElementMatcher.getMatches(e, queryEdges, DEFAULT_EDGE_LABEL)));
    return reuseTuple;
  }
}
