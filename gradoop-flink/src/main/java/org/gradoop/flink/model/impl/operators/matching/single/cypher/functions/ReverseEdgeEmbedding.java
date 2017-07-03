package org.gradoop.flink.model.impl.operators.matching.single.cypher.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

/**
 * Reverses an EdgeEmbedding, as it switches source and target
 * This is used for traversing incoming edges
 */
public class ReverseEdgeEmbedding extends RichMapFunction<Embedding, Embedding> {

  @Override
  public Embedding map(Embedding value) throws Exception {
    return value.reverse();
  }
}
