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
import org.gradoop.flink.model.impl.operators.matching.common.debug.PrintTripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.functions.TripleHasCandidate;
import org.gradoop.flink.model.impl.operators.matching.common.query.Step;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.debug.PrintEmbeddingWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions.UpdateVertexEdgeMapping;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

import static org.gradoop.flink.model.impl.operators.matching.common.debug.Printer.log;

/**
 * Extracts {@link Embedding}s iteratively from a given graph by traversing the
 * graph according to a given {@link TraversalCode}.
 *
 * For the iteration the traverser uses a basic for loop.
 *
 * @param <K> key type
 */
public class TripleForLoopTraverser<K> extends TripleTraverser<K> {

  /**
   * Creates a new distributed traverser.
   *
   * @param traversalCode describes the graph traversal
   * @param vertexCount   number of query vertices
   * @param edgeCount     number of query edges
   * @param keyClazz      key type for embedding initialization
   */
  public TripleForLoopTraverser(TraversalCode traversalCode,
    int vertexCount, int edgeCount, Class<K> keyClazz) {
    this(traversalCode, MatchStrategy.ISOMORPHISM,
      vertexCount, edgeCount, keyClazz,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES, null, null);
  }

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
  public TripleForLoopTraverser(TraversalCode traversalCode, MatchStrategy matchStrategy,
    int vertexCount, int edgeCount, Class<K> keyClazz,
    JoinOperatorBase.JoinHint edgeStepJoinStrategy,
    DataSet<Tuple2<K, PropertyValue>> vertexMapping,
    DataSet<Tuple2<K, PropertyValue>> edgeMapping) {
    super(traversalCode, matchStrategy, vertexCount, edgeCount, keyClazz, edgeStepJoinStrategy,
      vertexMapping, edgeMapping);
  }

  @Override
  public DataSet<Tuple1<Embedding<K>>> traverse(DataSet<TripleWithCandidates<K>> triples) {
    return iterate(triples, buildInitialEmbeddings(triples)).project(1);
  }

  @Override
  boolean isIterative() {
    return false;
  }

  /**
   * Explores the graph iteratively using the provided traversal code.
   *
   * @param triples    triple candidates
   * @param embeddings initial embeddings
   * @return final embeddings
   */
  private DataSet<EmbeddingWithTiePoint<K>> iterate(
    DataSet<TripleWithCandidates<K>> triples,
    DataSet<EmbeddingWithTiePoint<K>> embeddings) {

    TraversalCode traversalCode = getTraversalCode();

    for (int i = 1; i < traversalCode.getSteps().size(); i++) {
      Step step = traversalCode.getStep(i);

      embeddings = log(embeddings,
        new PrintEmbeddingWithTiePoint<>(i, "pre-join-embeddings"),
        getVertexMapping(), getEdgeMapping());

      DataSet<TripleWithCandidates<K>> filteredTriples = triples
        .filter(new TripleHasCandidate<>((int) step.getVia()));

      filteredTriples = log(filteredTriples,
        new PrintTripleWithCandidates<>(i, "pre-join-triples"),
        getVertexMapping(), getEdgeMapping());

      embeddings = embeddings
        .join(filteredTriples, getEdgeStepJoinStrategy())
        .where(0).equalTo(step.isOutgoing() ? 1 : 2)
        .with(new UpdateVertexEdgeMapping<>(traversalCode, i, getMatchStrategy()));
    }

    return log(embeddings,
      new PrintEmbeddingWithTiePoint<>(isIterative(), "final-embeddings"),
      getVertexMapping(), getEdgeMapping());
  }
}

