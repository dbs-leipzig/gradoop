
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

/**
 * Filters a set of embedding by given predicates
 */
public class FilterEmbedding extends RichFilterFunction<Embedding> {
  /**
   * Predicates used for filtering
   */
  private final CNF predicates;
  /**
   * Mapping of variables names to embedding column
   */
  private final EmbeddingMetaData metaData;

  /**
   * New embedding filter function
   *
   * @param predicates predicates used for filtering
   * @param metaData mapping of variable names to embedding column
   */
  public FilterEmbedding(CNF predicates, EmbeddingMetaData metaData) {
    this.predicates = predicates;
    this.metaData = metaData;
  }

  @Override
  public boolean filter(Embedding embedding) {
    return predicates.evaluate(embedding, metaData);
  }
}
