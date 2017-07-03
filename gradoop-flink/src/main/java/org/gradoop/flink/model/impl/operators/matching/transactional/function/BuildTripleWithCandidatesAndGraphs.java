
package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.flink.model.impl.operators.matching.common.functions.AbstractBuilder;
import org.gradoop.flink.model.impl.operators.matching.common.matching.ElementMatcher;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;

import java.util.Collection;

import static org.gradoop.common.util.GConstants.DEFAULT_EDGE_LABEL;

/**
 * Converts an EPGM edge to a Tuple2 with its graphs in field 0 and a
 * {@link TripleWithCandidates} in field 1.
 *
 * @param <E> EPGM edge type
 */
public class BuildTripleWithCandidatesAndGraphs<E extends Edge>
  extends AbstractBuilder<E, Tuple2<GradoopIdList, TripleWithCandidates<GradoopId>>> {

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
  private final Tuple2<GradoopIdList, TripleWithCandidates<GradoopId>>
    reuseTuple;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public BuildTripleWithCandidatesAndGraphs(String query) {
    super(query);
    reuseTuple = new Tuple2<>();
    reuseTuple.f1 = new TripleWithCandidates<>();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryEdges = getQueryHandler().getEdges();
    edgeCount = queryEdges.size();
  }

  @Override
  public Tuple2<GradoopIdList, TripleWithCandidates<GradoopId>> map(E e)
    throws Exception {
    reuseTuple.f0 = e.getGraphIds();
    reuseTuple.f1.setEdgeId(e.getId());
    reuseTuple.f1.setSourceId(e.getSourceId());
    reuseTuple.f1.setTargetId(e.getTargetId());
    reuseTuple.f1.setCandidates(getCandidates(edgeCount,
      ElementMatcher.getMatches(e, queryEdges, DEFAULT_EDGE_LABEL)));
    return reuseTuple;
  }
}

