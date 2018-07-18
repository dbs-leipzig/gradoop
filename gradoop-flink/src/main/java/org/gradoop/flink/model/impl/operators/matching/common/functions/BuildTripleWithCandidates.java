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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;

import java.util.Collection;

import static org.gradoop.common.util.GradoopConstants.DEFAULT_EDGE_LABEL;

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
