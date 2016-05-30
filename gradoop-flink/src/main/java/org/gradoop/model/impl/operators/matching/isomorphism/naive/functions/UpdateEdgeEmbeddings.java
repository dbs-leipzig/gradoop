package org.gradoop.model.impl.operators.matching.isomorphism.naive.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.query.Step;
import org.gradoop.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples
  .EdgeStep;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples
  .EmbeddingWithTiePoint;

/**
 * Extends an embedding with an edge if possible.
 *
 * Read fields first:
 *
 * f0.f1: edge embeddings
 *
 * Read fields second:
 *
 * f0: edge id
 * f2: next id
 *
 * Forwarded fields first:
 *
 * f0.f0: vertex embeddings
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
public class UpdateEdgeEmbeddings extends
  RichFlatJoinFunction<EmbeddingWithTiePoint, EdgeStep, EmbeddingWithTiePoint> {

  private final TraversalCode traversalCode;

  private Step step;

  public UpdateEdgeEmbeddings(TraversalCode traversalCode) {
    this.traversalCode = traversalCode;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    step = traversalCode.getStep(
      getIterationRuntimeContext().getSuperstepNumber() - 1);
  }

  @Override
  public void join(EmbeddingWithTiePoint embedding, EdgeStep edgeStep,
    Collector<EmbeddingWithTiePoint> collector) throws Exception {

    GradoopId[] edgeEmbeddings = embedding.getEmbedding().getEdgeEmbeddings();

    // traverse if no edge set for that step
    if (edgeEmbeddings[(int) step.getVia()] == null) {
      edgeEmbeddings[(int) step.getVia()] = edgeStep.getEdgeId();
      embedding.getEmbedding().setEdgeEmbeddings(edgeEmbeddings);
      embedding.setTiePointId(edgeStep.getNextId());
      collector.collect(embedding);
    }
  }
}
