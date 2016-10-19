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

package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.functions.ElementHasCandidate;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.debug.PrintEmbeddingWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.BuildEmbeddingWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

import java.util.Objects;

import static org.gradoop.flink.model.impl.operators.matching.common.debug.Printer.log;

/**
 * A distributed traverser extracts embeddings from a given graph.
 *
 * @param <K> key type
 */
public abstract class DistributedTraverser<K> {
  /**
   * Strategy for vertex and edge mappings
   */
  private final MatchStrategy matchStrategy;
  /**
   * Controls the graph traversal
   */
  private final TraversalCode traversalCode;
  /**
   * Number of vertices in the query graph.
   */
  private final int vertexCount;
  /**
   * Number of edges in the query graph.
   */
  private final int edgeCount;
  /**
   * Needed to build initial embeddings
   */
  private final Class<K> keyClazz;
  /**
   * Join strategy used for the join between embeddings and edges
   */
  private final JoinOperatorBase.JoinHint edgeStepJoinStrategy;
  /**
   * Join strategy used for the join between embeddings and vertices
   */
  private final JoinOperatorBase.JoinHint vertexStepJoinStrategy;
  /**
   * Vertex mapping used for debug
   */
  private final DataSet<Tuple2<K, PropertyValue>> vertexMapping;
  /**
   * Edge mapping used for debug
   */
  private final DataSet<Tuple2<K, PropertyValue>> edgeMapping;

  /**
   * Creates a new distributed traverser.
   *
   * @param traversalCode          describes the graph traversal
   * @param matchStrategy          matching strategy for vertices and edges
   * @param vertexCount            number of query vertices
   * @param edgeCount              number of query edges
   * @param keyClazz               key type for embedding initialization
   * @param edgeStepJoinStrategy   Join strategy for edge extension
   * @param vertexStepJoinStrategy Join strategy for vertex extension
   * @param vertexMapping          used for debug
   * @param edgeMapping            used for debug
   */
  DistributedTraverser(TraversalCode traversalCode,
    MatchStrategy matchStrategy,
    int vertexCount, int edgeCount,
    Class<K> keyClazz,
    JoinOperatorBase.JoinHint edgeStepJoinStrategy,
    JoinOperatorBase.JoinHint vertexStepJoinStrategy,
    DataSet<Tuple2<K, PropertyValue>> vertexMapping,
    DataSet<Tuple2<K, PropertyValue>> edgeMapping) {

    Objects.requireNonNull(traversalCode);
    Objects.requireNonNull(matchStrategy);
    Objects.requireNonNull(keyClazz);
    Objects.requireNonNull(edgeStepJoinStrategy);
    Objects.requireNonNull(vertexStepJoinStrategy);

    this.traversalCode          = traversalCode;
    this.matchStrategy          = matchStrategy;
    this.vertexCount            = vertexCount;
    this.edgeCount              = edgeCount;
    this.keyClazz               = keyClazz;
    this.edgeStepJoinStrategy   = edgeStepJoinStrategy;
    this.vertexStepJoinStrategy = vertexStepJoinStrategy;
    this.vertexMapping          = vertexMapping;
    this.edgeMapping            = edgeMapping;
  }

  /**
   * Traverses the graph, thereby extracting embeddings of a given pattern.
   *
   * @param vertices  vertices including their query candidates
   * @param edges     edges including their query candidates
   * @return embeddings contained in the graph
   */
  public abstract DataSet<Tuple1<Embedding<K>>> traverse(
    DataSet<IdWithCandidates<K>> vertices,
    DataSet<TripleWithCandidates<K>> edges);

  TraversalCode getTraversalCode() {
    return traversalCode;
  }

  MatchStrategy getMatchStrategy() {
    return matchStrategy;
  }

  JoinOperatorBase.JoinHint getEdgeStepJoinStrategy() {
    return edgeStepJoinStrategy;
  }

  JoinOperatorBase.JoinHint getVertexStepJoinStrategy() {
    return vertexStepJoinStrategy;
  }

  DataSet<Tuple2<K, PropertyValue>> getVertexMapping() {
    return vertexMapping;
  }

  DataSet<Tuple2<K, PropertyValue>> getEdgeMapping() {
    return edgeMapping;
  }

  /**
   * Builds the initial embeddings from the given vertices.
   *
   * @param vertices vertices and their query candidates
   * @return initial embeddings
   */
  DataSet<EmbeddingWithTiePoint<K>> buildInitialEmbeddings(
    DataSet<IdWithCandidates<K>> vertices) {

    DataSet<EmbeddingWithTiePoint<K>> initialEmbeddings = vertices
      .filter(new ElementHasCandidate<>(
        getTraversalCode().getStep(0).getFrom()))
      .map(new BuildEmbeddingWithTiePoint<>(keyClazz, getTraversalCode(),
        vertexCount, edgeCount));

    return log(initialEmbeddings, new PrintEmbeddingWithTiePoint<>(),
      getVertexMapping(), getEdgeMapping());
  }
}
