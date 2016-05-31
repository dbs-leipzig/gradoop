package org.gradoop.model.impl.operators.matching.isomorphism.naive.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples.EmbeddingWithTiePoint;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples.VertexStep;

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

  private int stepCount;

  private int candidate;

  public UpdateVertexMappings(TraversalCode traversalCode) {
    this.traversalCode  = traversalCode;
    this.stepCount      = traversalCode.getSteps().size();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    currentStep = getIterationRuntimeContext().getSuperstepNumber() - 1;
    candidate = (int) traversalCode.getStep(currentStep).getTo();
  }

  @Override
  public void join(EmbeddingWithTiePoint embedding, VertexStep vertexStep,
    Collector<EmbeddingWithTiePoint> collector) throws Exception {

    GradoopId[] vertexMappings = embedding.getEmbedding().getVertexMappings();

    // not seen before or same as seen before (bijection)
    if (vertexMappings[candidate] == null ||
      vertexMappings[candidate].equals(vertexStep.getVertexId())) {
      vertexMappings[candidate] = vertexStep.getVertexId();
      embedding.getEmbedding().setVertexMappings(vertexMappings);

      // set next tie point if there are more steps
      if (currentStep < stepCount - 1) {
        int nextFrom = (int) traversalCode.getStep(currentStep + 1).getFrom();
        embedding.setTiePointId(vertexMappings[nextFrom]);
      }
      collector.collect(embedding);
    }
  }
}
