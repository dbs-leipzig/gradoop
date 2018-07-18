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
package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.matching.ElementMatcher;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Collection;

import static org.gradoop.common.util.GradoopConstants.DEFAULT_VERTEX_LABEL;

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
  extends AbstractBuilder<V, IdWithCandidates<GradoopId>> {
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
  private final IdWithCandidates<GradoopId> reuseTuple;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public BuildIdWithCandidates(String query) {
    super(query);
    reuseTuple = new IdWithCandidates<>();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryVertices = getQueryHandler().getVertices();
    vertexCount   = queryVertices.size();
  }

  @Override
  public IdWithCandidates<GradoopId> map(V v) throws Exception {
    reuseTuple.setId(v.getId());
    reuseTuple.setCandidates(getCandidates(vertexCount,
      ElementMatcher.getMatches(v, queryVertices, DEFAULT_VERTEX_LABEL)));
    return reuseTuple;
  }
}
