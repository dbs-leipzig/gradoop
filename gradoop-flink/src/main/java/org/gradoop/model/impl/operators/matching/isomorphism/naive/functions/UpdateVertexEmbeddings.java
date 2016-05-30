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
 * f0.f0: vertex embeddings
 *
 * Read fields second:
 *
 * f0: vertex id
 *
 * Forwarded fields first:
 *
 * f0.f1: edge embeddings
 *
 */
@FunctionAnnotation.ReadFieldsFirst("f0.f0")
@FunctionAnnotation.ReadFieldsSecond("f0")
public class UpdateVertexEmbeddings extends RichFlatJoinFunction
  <EmbeddingWithTiePoint, VertexStep, EmbeddingWithTiePoint> {

  private final TraversalCode traversalCode;

  private int currentStep;

  private int stepCount;

  private int candidate;

  public UpdateVertexEmbeddings(TraversalCode traversalCode) {
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

    GradoopId[] vertexEmbeddings = embedding.getEmbedding().getVertexEmbeddings();

    // not seen before or same as seen before (bijection)
    if (vertexEmbeddings[candidate] == null ||
      vertexEmbeddings[candidate].equals(vertexStep.getVertexId())) {
      vertexEmbeddings[candidate] = vertexStep.getVertexId();
      embedding.getEmbedding().setVertexEmbeddings(vertexEmbeddings);

      // set next tie point if there are more steps
      if (currentStep < stepCount - 1) {
        int nextFrom = (int) traversalCode.getStep(currentStep + 1).getFrom();
        embedding.setTiePointId(vertexEmbeddings[nextFrom]);
      }
      collector.collect(embedding);
    }
  }
}
