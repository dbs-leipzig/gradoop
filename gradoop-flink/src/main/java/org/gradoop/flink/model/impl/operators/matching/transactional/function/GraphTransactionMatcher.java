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
import org.gradoop.flink.representation.transaction.GraphTransaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.gradoop.common.util.GConstants.DEFAULT_EDGE_LABEL;
import static org.gradoop.common.util.GConstants.DEFAULT_VERTEX_LABEL;

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
