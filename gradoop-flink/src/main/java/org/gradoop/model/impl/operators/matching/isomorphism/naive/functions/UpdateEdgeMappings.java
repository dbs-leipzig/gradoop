package org.gradoop.model.impl.operators.matching.isomorphism.naive.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples.EdgeStep;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples.EmbeddingWithTiePoint;

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

  private final TraversalCode traversalCode;

  private int candidate;

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
