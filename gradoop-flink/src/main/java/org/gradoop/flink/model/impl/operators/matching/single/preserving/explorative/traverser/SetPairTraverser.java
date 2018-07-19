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
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.functions.ElementHasCandidate;
import org.gradoop.flink.model.impl.operators.matching.common.query.Step;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.ExplorativePatternMatching;

import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.debug.PrintEdgeStep;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.debug.PrintEmbeddingWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.debug.PrintVertexStep;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.BuildEdgeStep;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.BuildEmbeddingFromVertex;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.BuildVertexStep;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.EdgeHasCandidate;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.UpdateEdgeMapping;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.UpdateVertexMapping;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.VertexHasCandidate;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EdgeStep;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.VertexStep;

import java.util.Objects;

import static org.gradoop.flink.model.impl.operators.matching.common.debug.Printer.log;

/**
 * Traverses a graph represented by two DataSets (vertices and edges).
 *
 * @param <K> key type
 */
public abstract class SetPairTraverser<K> extends DistributedTraverser<K> {
  /**
   * Join strategy used for the join between embeddings and edges
   */
  private final JoinOperatorBase.JoinHint edgeStepJoinStrategy;
  /**
   * Join strategy used for the join between embeddings and vertices
   */
  private final JoinOperatorBase.JoinHint vertexStepJoinStrategy;

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
  SetPairTraverser(TraversalCode traversalCode, MatchStrategy matchStrategy, int vertexCount,
    int edgeCount, Class<K> keyClazz, JoinOperatorBase.JoinHint edgeStepJoinStrategy,
    JoinOperatorBase.JoinHint vertexStepJoinStrategy,
    DataSet<Tuple2<K, PropertyValue>> vertexMapping,
    DataSet<Tuple2<K, PropertyValue>> edgeMapping) {
    super(traversalCode, matchStrategy, vertexCount, edgeCount, keyClazz,
      vertexMapping, edgeMapping);

    Objects.requireNonNull(edgeStepJoinStrategy);
    Objects.requireNonNull(vertexStepJoinStrategy);

    this.edgeStepJoinStrategy = edgeStepJoinStrategy;
    this.vertexStepJoinStrategy = vertexStepJoinStrategy;
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

  private JoinOperatorBase.JoinHint getEdgeStepJoinStrategy() {
    return edgeStepJoinStrategy;
  }

  private JoinOperatorBase.JoinHint getVertexStepJoinStrategy() {
    return vertexStepJoinStrategy;
  }

  /**
   * Builds the initial embeddings from the given vertices.
   *
   * @param vertices vertices and their query candidates
   * @return initial embeddings
   */
  DataSet<EmbeddingWithTiePoint<K>> buildInitialEmbeddings(DataSet<IdWithCandidates<K>> vertices) {

    Step initialStep = getTraversalCode().getStep(0);

    DataSet<EmbeddingWithTiePoint<K>> initialEmbeddings = vertices
      .filter(new ElementHasCandidate<>((int) initialStep.getFrom()))
      .map(new BuildEmbeddingFromVertex<>(getKeyClazz(), initialStep,
        getVertexCount(), getEdgeCount()));

    return log(initialEmbeddings,
      new PrintEmbeddingWithTiePoint<>(), getVertexMapping(), getEdgeMapping());
  }

  /**
   * Extends the given embeddings with valid edges and returns the updated
   * embeddings.
   *
   * @param edges             edges including their candidates
   * @param embeddings        embeddings
   * @param superstep         current iteration
   * @param traverserStrategy iteration strategy
   * @param forwardedFields   forwarded fields for edge step creation
   * @return updated embeddings
   */
  DataSet<EmbeddingWithTiePoint<K>> traverseEdges(
    DataSet<TripleWithCandidates<K>> edges,
    DataSet<EmbeddingWithTiePoint<K>> embeddings,
    DataSet<Integer> superstep,
    TraverserStrategy traverserStrategy,
    String[] forwardedFields) {

    DataSet<EdgeStep<K>> edgeSteps = edges
      .filter(new EdgeHasCandidate<>(getTraversalCode()))
      .withBroadcastSet(superstep, ExplorativePatternMatching.BC_SUPERSTEP)
      .map(new BuildEdgeStep<>(getTraversalCode()))
      .withBroadcastSet(superstep, ExplorativePatternMatching.BC_SUPERSTEP)
      .withForwardedFields(forwardedFields);

    edgeSteps = log(edgeSteps,
      new PrintEdgeStep<>(isIterative(), "post-filter-map-edge"),
      getVertexMapping(), getEdgeMapping());

    JoinOperator<EmbeddingWithTiePoint<K>, EdgeStep<K>,
      EmbeddingWithTiePoint<K>> join = embeddings
      .join(edgeSteps, getEdgeStepJoinStrategy())
      .where(0).equalTo(1) // tiePointId == sourceId/targetId tie point
      .with(new UpdateEdgeMapping<>(getTraversalCode(), getMatchStrategy(), traverserStrategy));

    if (traverserStrategy == TraverserStrategy.SET_PAIR_FOR_LOOP_ITERATION) {
      embeddings = join.withBroadcastSet(superstep, ExplorativePatternMatching.BC_SUPERSTEP);
    } else {
      embeddings = join;
    }

    return log(embeddings,
      new PrintEmbeddingWithTiePoint<>(isIterative(), "post-edge-update"),
      getVertexMapping(), getEdgeMapping());
  }

  /**
   * Extends the given embeddings with valid vertices and returns the updated
   * embeddings.
   *
   * @param vertices          vertices including their candidates
   * @param embeddings        embeddings
   * @param superstep         current super step
   * @param traverserStrategy iteration strategy
   * @return updated embeddings
   */
  DataSet<EmbeddingWithTiePoint<K>> traverseVertices(
    DataSet<IdWithCandidates<K>> vertices,
    DataSet<EmbeddingWithTiePoint<K>> embeddings,
    DataSet<Integer> superstep,
    TraverserStrategy traverserStrategy) {

    DataSet<VertexStep<K>> vertexSteps = vertices
      .filter(new VertexHasCandidate<>(getTraversalCode()))
      .withBroadcastSet(superstep, ExplorativePatternMatching.BC_SUPERSTEP)
      .map(new BuildVertexStep<>());

    vertexSteps = log(vertexSteps,
      new PrintVertexStep<>(isIterative(), "post-filter-project-vertex"),
      getVertexMapping(), getEdgeMapping());

    JoinOperator<EmbeddingWithTiePoint<K>, VertexStep<K>,
      EmbeddingWithTiePoint<K>> join = embeddings
      .join(vertexSteps, getVertexStepJoinStrategy())
      .where(0).equalTo(0) // tiePointId == vertexId
      .with(new UpdateVertexMapping<>(getTraversalCode(), getMatchStrategy(), traverserStrategy));

    if (traverserStrategy == TraverserStrategy.SET_PAIR_FOR_LOOP_ITERATION) {
      embeddings = join.withBroadcastSet(superstep, ExplorativePatternMatching.BC_SUPERSTEP);
    } else {
      embeddings = join;
    }

    embeddings = log(embeddings,
      new PrintEmbeddingWithTiePoint<>(isIterative(), "post-vertex-update"),
      getVertexMapping(), getEdgeMapping());
    return embeddings;
  }
}
