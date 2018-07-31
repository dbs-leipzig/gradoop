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
