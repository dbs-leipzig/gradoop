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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.query.Step;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.tuples.EmbeddingWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.tuples.VertexStep;

/**
 * Extends an embedding with a vertex if possible.
 *
 * Read fields first:
 *
 * f0.f0: vertex mappings
 *
 * Read fields second:
 *
 * f0: vertex id
 *
 * Forwarded fields first:
 *
 * f0.f1: edge mappings
 *
 */
@FunctionAnnotation.ReadFieldsFirst("f0.f0")
@FunctionAnnotation.ReadFieldsSecond("f0")
public class UpdateVertexMappings extends RichFlatJoinFunction
  <EmbeddingWithTiePoint, VertexStep, EmbeddingWithTiePoint> {
  /**
   * Traversal code
   */
  private final TraversalCode traversalCode;
  /**
   * Current step in the traversal
   */
  private int currentStep;
  /**
   * From field of the next step in the traversal (if not last)
   */
  private int nextFrom;
  /**
   * Total number of steps in the traversal
   */
  private int stepCount;
  /**
   * Index to check in the vertex mapping
   */
  private int candidate;
  /**
   * From fields of the previous steps (used for faster checking)
   */
  private int[] previousFroms;

  /**
   * Constructor
   *
   * @param tc traversal code for the current exploration
   */
  public UpdateVertexMappings(TraversalCode tc) {
    this.traversalCode  = tc;
    this.stepCount      = tc.getSteps().size();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    currentStep = getIterationRuntimeContext().getSuperstepNumber() - 1;
    candidate = (int) traversalCode.getStep(currentStep).getTo();

    if (hasMoreSteps()) {
      this.nextFrom = (int) traversalCode.getStep(currentStep + 1).getFrom();
    }

    previousFroms = new int[currentStep + 1];
    for (int i = 0; i <= currentStep; i++) {
      Step s = traversalCode.getStep(i);
      previousFroms[i] = (int) s.getFrom();
    }
  }

  @Override
  public void join(EmbeddingWithTiePoint embedding, VertexStep vertexStep,
    Collector<EmbeddingWithTiePoint> collector) throws Exception {

    GradoopId[] vertexMappings = embedding.getEmbedding().getVertexMappings();

    GradoopId vertexId = vertexStep.getVertexId();
    boolean isMapped = vertexMappings[candidate] != null;

    // not seen before or same as seen before (ensure bijection)
    if ((!isMapped && !seenBefore(vertexMappings, vertexId)) ||
      (isMapped && vertexMappings[candidate].equals(vertexId))) {

      vertexMappings[candidate] = vertexId;
      embedding.getEmbedding().setVertexMappings(vertexMappings);

      // set next tie point if there are more steps in the traversal
      if (hasMoreSteps()) {
        embedding.setTiePointId(vertexMappings[nextFrom]);
      }
      collector.collect(embedding);
    }
  }

  /**
   * Check if there are more traversal steps left.
   *
   * @return true, if there are more steps
   */
  private boolean hasMoreSteps() {
    return currentStep < stepCount - 1;
  }

  /**
   * Check if the given id has been visited before.
   *
   * @param vertexMappings  current vertex mappings
   * @param id              current vertex id
   * @return true, if visited before
   */
  private boolean seenBefore(GradoopId[] vertexMappings, GradoopId id) {
    boolean result = false;
    for (int i = 0; i <= currentStep; i++) {
      if (vertexMappings[previousFroms[i]].equals(id)) {
        result = true;
        break;
      }
    }
    return result;
  }
}
