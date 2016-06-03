package org.gradoop.model.impl.operators.matching.isomorphism.explorative.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.query.Step;
import org.gradoop.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.tuples.EmbeddingWithTiePoint;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.tuples.VertexStep;

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

  private final TraversalCode traversalCode;

  private int currentStep;

  private int nextFrom;

  private int stepCount;

  private int candidate;

  private int[] previousFroms;

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

    // not seen before or same as seen before (bijection)
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

  private boolean hasMoreSteps() {
    return currentStep < stepCount - 1;
  }

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
