package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.add.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.List;

/**
 * Adds {@link Embedding} columns based on a given set of variables and element types.
 */
public class AddEmbeddingElements extends RichMapFunction<Embedding, Embedding> {
  /**
   * Return pattern variables that do not exist in pattern matching query
   */
  private final List<String> newReturnPatternVariables;

  /**
   * New embeddings add function
   *
   * @param newReturnPatternVariables Difference between query and return pattern variables
   */
  public AddEmbeddingElements(List<String> newReturnPatternVariables) {
    this.newReturnPatternVariables = newReturnPatternVariables;
  }

  @Override
  public Embedding map(Embedding embedding) throws Exception {
    for (String var : newReturnPatternVariables) {
      GradoopId id = GradoopId.get();
      embedding.add(id);
    }
    return embedding;
  }
}
