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
import org.gradoop.flink.model.impl.operators.matching.common.query.Step;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

/**
 * Extracts {@link Embedding}s iteratively from a given graph by traversing the
 * graph according to a given {@link TraversalCode}.
 *
 * For the iteration the traverser uses a basic for loop.
 *
 * @param <K> key type
 */
public class SetPairForLoopTraverser<K> extends SetPairTraverser<K> {

  /**
   * Creates a new distributed traverser.
   *
   * @param traversalCode describes the traversal through the graph
   * @param vertexCount   number of query vertices
   * @param edgeCount     number of query edges
   * @param keyClazz      needed for embedding initialization
   */
  public SetPairForLoopTraverser(TraversalCode traversalCode,
    int vertexCount, int edgeCount, Class<K> keyClazz) {
    this(traversalCode, MatchStrategy.ISOMORPHISM,
      vertexCount, edgeCount,
      keyClazz,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES,
      null, null); // debug mappings
  }

  /**
   * Creates a new distributed traverser.
   *
   * @param traversalCode          describes the graph traversal
   * @param matchStrategy          matching strategy for vertex and edge mapping
   * @param vertexCount            number of query vertices
   * @param edgeCount              number of query edges
   * @param keyClazz               key type for embedding initialization
   * @param edgeStepJoinStrategy   Join strategy for edge extension
   * @param vertexStepJoinStrategy Join strategy for vertex extension
   * @param vertexMapping          used for debug
   * @param edgeMapping            used for debug
   */
  public SetPairForLoopTraverser(TraversalCode traversalCode,
    MatchStrategy matchStrategy,
    int vertexCount, int edgeCount,
    Class<K> keyClazz,
    JoinOperatorBase.JoinHint edgeStepJoinStrategy,
    JoinOperatorBase.JoinHint vertexStepJoinStrategy,
    DataSet<Tuple2<K, PropertyValue>> vertexMapping,
    DataSet<Tuple2<K, PropertyValue>> edgeMapping) {
    super(traversalCode, matchStrategy, vertexCount, edgeCount, keyClazz,
      edgeStepJoinStrategy, vertexStepJoinStrategy, vertexMapping, edgeMapping);
  }

  @Override
  public DataSet<Tuple1<Embedding<K>>> traverse(
    DataSet<IdWithCandidates<K>> vertices,
    DataSet<TripleWithCandidates<K>> edges) {
    return iterate(vertices, edges, buildInitialEmbeddings(vertices)).project(1);
  }

  @Override
  boolean isIterative() {
    return false;
  }

  /**
   * Explores the data graph iteratively using the provided traversal code.
   *
   * @param vertices   vertex candidates
   * @param edges      edge candidates
   * @param embeddings initial embeddings which are extended in each iteration
   * @return final embeddings
   */
  private DataSet<EmbeddingWithTiePoint<K>> iterate(
    DataSet<IdWithCandidates<K>> vertices,
    DataSet<TripleWithCandidates<K>> edges,
    DataSet<EmbeddingWithTiePoint<K>> embeddings) {

    TraversalCode traversalCode = getTraversalCode();

    for (int i = 0; i < traversalCode.getSteps().size(); i++) {
      Step step = traversalCode.getStep(i);

      DataSet<Integer> superstep = vertices.getExecutionEnvironment()
        .fromElements(i + 1);

      String[] forwardedFieldEdgeSteps = new String[] {
          // forward edge id
          "f0",
          // forward source or target id to tie point id
          step.isOutgoing() ? "f1->f1" : "f2->f1",
          // forward source or target id to next id
          step.isOutgoing() ? "f2->f2" : "f1->f2"
      };

      embeddings = traverseEdges(edges, embeddings, superstep,
        TraverserStrategy.SET_PAIR_FOR_LOOP_ITERATION, forwardedFieldEdgeSteps);
      embeddings = traverseVertices(vertices, embeddings, superstep,
        TraverserStrategy.SET_PAIR_FOR_LOOP_ITERATION);
    }

    return embeddings;
  }
}
