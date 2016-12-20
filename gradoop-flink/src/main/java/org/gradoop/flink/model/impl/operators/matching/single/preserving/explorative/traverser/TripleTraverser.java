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
import org.gradoop.flink.model.impl.operators.matching.common.functions.TripleHasCandidate;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.BuildEmbeddingFromTriple;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

import java.util.Objects;

/**
 * Traverses a graph represented by one DataSets containing triplets.
 *
 * @param <K> key type
 */
public abstract class TripleTraverser<K> extends DistributedTraverser<K> {
  /**
   * Join strategy used for the join between embeddings and edges
   */
  private final JoinOperatorBase.JoinHint edgeStepJoinStrategy;

  /**
   * Creates a new distributed traverser.
   *
   * @param traversalCode        describes the graph traversal
   * @param matchStrategy        matching strategy for vertices and edges
   * @param vertexCount          number of query vertices
   * @param edgeCount            number of query edges
   * @param keyClazz             key type for embedding initialization
   * @param edgeStepJoinStrategy Join strategy for edge extension
   * @param vertexMapping        used for debug
   * @param edgeMapping          used for debug
   */
  TripleTraverser(TraversalCode traversalCode, MatchStrategy matchStrategy, int vertexCount,
    int edgeCount, Class<K> keyClazz,
    JoinOperatorBase.JoinHint edgeStepJoinStrategy,
    DataSet<Tuple2<K, PropertyValue>> vertexMapping,
    DataSet<Tuple2<K, PropertyValue>> edgeMapping) {
    super(traversalCode, matchStrategy, vertexCount, edgeCount, keyClazz, vertexMapping,
      edgeMapping);

    Objects.requireNonNull(edgeStepJoinStrategy);
    this.edgeStepJoinStrategy = edgeStepJoinStrategy;
  }

  JoinOperatorBase.JoinHint getEdgeStepJoinStrategy() {
    return edgeStepJoinStrategy;
  }

  /**
   * Traverses the graph, thereby extracting embeddings of a given pattern.
   *
   * @param triples triples representing the data graph
   * @return embeddings contained in the graph
   */
  public abstract DataSet<Tuple1<Embedding<K>>> traverse(DataSet<TripleWithCandidates<K>> triples);

  /**
   * Initialize the embeddings from the given edge triples.
   *
   * @param t edge triple candidates for the first step in the traversal
   * @return initial embeddings
   */
  DataSet<EmbeddingWithTiePoint<K>> buildInitialEmbeddings(DataSet<TripleWithCandidates<K>> t) {
    return t
      .filter(new TripleHasCandidate<>((int) getTraversalCode().getStep(0).getVia()))
      .flatMap(new BuildEmbeddingFromTriple<>(getKeyClazz(), getTraversalCode(), getMatchStrategy(),
        getVertexCount(), getEdgeCount()));
  }
}
