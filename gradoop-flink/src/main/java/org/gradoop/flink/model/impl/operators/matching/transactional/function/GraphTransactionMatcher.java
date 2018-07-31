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
package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.common.matching.ElementMatcher;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.transactional.tuples.GraphWithCandidates;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.gradoop.common.util.GradoopConstants.DEFAULT_EDGE_LABEL;
import static org.gradoop.common.util.GradoopConstants.DEFAULT_VERTEX_LABEL;

/**
 * Matches the elements of GraphTransactions against a query string. Returns
 * GraphsWithCandidates, which can be input for HasEmbeddings or FindEmbeddings.
 */
public class GraphTransactionMatcher
  extends RichMapFunction<GraphTransaction, GraphWithCandidates> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;
  /**
   * Query handler
   */
  private transient QueryHandler handler;
  /**
   * Query string
   */
  private String query;

  /**
   * Constructor
   * @param query query string
   */
  public GraphTransactionMatcher(String query) {
    this.query = query;
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    handler = new QueryHandler(this.query);
  }

  @Override
  public GraphWithCandidates map(GraphTransaction graphTransaction) {

    GraphWithCandidates graph = new GraphWithCandidates(
      graphTransaction.getGraphHead().getId());

    Set<Vertex> vertices = graphTransaction.getVertices();
    List<IdWithCandidates<GradoopId>> vertexCandidates
      = new ArrayList<>(vertices.size());

    for (Vertex vertex : vertices) {
      IdWithCandidates<GradoopId> candidates = new IdWithCandidates<>();
      candidates.setId(vertex.getId());
      candidates.setCandidates(
        getCandidates(handler.getVertexCount(), ElementMatcher.getMatches(
            vertex,
            handler.getVertices(),
            DEFAULT_VERTEX_LABEL
          )
        )
      );
      vertexCandidates.add(candidates);
    }

    Set<Edge> edges = graphTransaction.getEdges();
    List<TripleWithCandidates<GradoopId>> edgeCandidates =
    new ArrayList<>(edges.size());

    for (Edge edge : edges) {
      TripleWithCandidates<GradoopId> candidates = new TripleWithCandidates<>();
      candidates.setEdgeId(edge.getId());
      candidates.setSourceId(edge.getSourceId());
      candidates.setTargetId(edge.getTargetId());
      candidates.setCandidates(
        getCandidates(handler.getEdgeCount(), ElementMatcher.getMatches(
            edge,
            handler.getEdges(),
            DEFAULT_EDGE_LABEL
          )
        )
      );
      edgeCandidates.add(candidates);
    }

    graph.setVertexCandidates(vertexCandidates);
    graph.setEdgeCandidates(edgeCandidates);
    return graph;
  }

  /**
   * Returns a bit vector representing the matches for the given entity.
   *
   * @param candidateCount  size of the bit vector
   * @param matches         matches for the given entity
   * @return bit vector representing matches
   */
  private boolean[] getCandidates(int candidateCount, List<Long> matches) {
    boolean[] candidates = new boolean[candidateCount];

    for (Long candidate : matches) {
      candidates[candidate.intValue()] = true;
    }
    return candidates;
  }
}
