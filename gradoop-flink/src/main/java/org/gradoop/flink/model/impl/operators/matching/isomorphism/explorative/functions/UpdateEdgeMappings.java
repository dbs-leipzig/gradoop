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

package org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.isomorphism
  .explorative.tuples.EdgeStep;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;

import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.tuples.EmbeddingWithTiePoint;

/**
 * Extends an embedding with an edge if possible.
 *
 * Read fields first:
 *
 * f0.f1: edge mappings
 *
 * Read fields second:
 *
 * f0: edge id
 * f2: next id
 *
 * Forwarded fields first:
 *
 * f0.f0: vertex mappings
 *
 * Forwarded fields second:
 *
 * f2->f1: next id -> tie point id
 *
 */
@FunctionAnnotation.ReadFieldsFirst("f0.f1")
@FunctionAnnotation.ReadFieldsSecond("f0;f2")
@FunctionAnnotation.ForwardedFieldsFirst("f0.f0")
@FunctionAnnotation.ForwardedFieldsSecond("f2->f1")
public class UpdateEdgeMappings extends
  RichFlatJoinFunction<EmbeddingWithTiePoint, EdgeStep, EmbeddingWithTiePoint> {
  /**
   * Traversal code for the current exploration
   */
  private final TraversalCode traversalCode;
  /**
   * Index to check in the edge mapping
   */
  private int candidate;

  /**
   * Constructor
   *
   * @param traversalCode traversal code for the current exploration
   */
  public UpdateEdgeMappings(TraversalCode traversalCode) {
    this.traversalCode = traversalCode;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    candidate = (int) traversalCode.getStep(
      getIterationRuntimeContext().getSuperstepNumber() - 1).getVia();
  }

  @Override
  public void join(EmbeddingWithTiePoint embedding, EdgeStep edgeStep,
    Collector<EmbeddingWithTiePoint> collector) throws Exception {

    GradoopId[] edgeEmbeddings = embedding.getEmbedding().getEdgeMappings();

    // traverse if no edge set for that step
    if (edgeEmbeddings[candidate] == null) {
      edgeEmbeddings[candidate] = edgeStep.getEdgeId();
      embedding.getEmbedding().setEdgeMappings(edgeEmbeddings);
      embedding.setTiePointId(edgeStep.getNextId());
      collector.collect(embedding);
    }
  }
}
