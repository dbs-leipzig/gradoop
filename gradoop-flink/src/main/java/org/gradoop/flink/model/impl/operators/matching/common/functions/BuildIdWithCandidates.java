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

package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.model.impl.operators.matching.common.matching
  .EntityMatcher;
import org.gradoop.flink.model.impl.operators.matching.common.tuples
  .IdWithCandidates;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Collection;

/**
 * Converts an EPGM vertex to a {@link IdWithCandidates} tuple.
 *
 * vertex -> (vertexId, vertexCandidates)
 *
 * Forwarded Fields:
 *
 * id->f0: vertex id
 *
 * @param <V> EPGM vertex type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class BuildIdWithCandidates<V extends Vertex>
  extends AbstractBuilder<V, IdWithCandidates> {
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
  private final IdWithCandidates reuseTuple;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public BuildIdWithCandidates(String query) {
    super(query);
    reuseTuple = new IdWithCandidates();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryVertices = getQueryHandler().getVertices();
    vertexCount   = queryVertices.size();
  }

  @Override
  public IdWithCandidates map(V v) throws Exception {
    reuseTuple.setId(v.getId());
    reuseTuple.setCandidates(getCandidates(vertexCount,
      EntityMatcher.getMatches(v, queryVertices)));
    return reuseTuple;
  }
}
