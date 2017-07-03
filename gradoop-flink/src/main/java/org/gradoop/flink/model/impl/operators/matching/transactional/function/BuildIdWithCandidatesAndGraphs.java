
package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.common.functions.AbstractBuilder;
import org.gradoop.flink.model.impl.operators.matching.common.matching.ElementMatcher;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;

import java.util.Collection;

import static org.gradoop.common.util.GConstants.DEFAULT_VERTEX_LABEL;

/**
 * Converts an EPGM vertex to a Tuple2 with its graph ids in field 0 and a
 * {@link IdWithCandidates} in field 1.
 *
 * (vId,props,graphs) -> {(graphId, (vid,candidates)}
 *
 * @param <V> EPGM vertex type
 */
public class BuildIdWithCandidatesAndGraphs<V extends Vertex>
  extends AbstractBuilder<V, Tuple2<GradoopIdList, IdWithCandidates<GradoopId>>> {
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;
  /**
   * Query vertices to match against.
   */
  private transient Collection<org.s1ck.gdl.model.Vertex> queryVertices;
  /**
   * Number of vertices in the query graph
   */
  private int vertexCount;
  /**
   * Reduce instantiations
   */
  private final Tuple2<GradoopIdList, IdWithCandidates<GradoopId>> reuseTuple;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public BuildIdWithCandidatesAndGraphs(String query) {
    super(query);
    reuseTuple = new Tuple2<>();
    reuseTuple.f1 = new IdWithCandidates<>();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryVertices = getQueryHandler().getVertices();
    vertexCount   = queryVertices.size();
  }

  @Override
  public Tuple2<GradoopIdList, IdWithCandidates<GradoopId>> map(V v)
    throws Exception {
    reuseTuple.f0 = v.getGraphIds();
    reuseTuple.f1.setId(v.getId());
    reuseTuple.f1.setCandidates(getCandidates(vertexCount,
      ElementMatcher.getMatches(v, queryVertices, DEFAULT_VERTEX_LABEL)));
    return reuseTuple;
  }
}
