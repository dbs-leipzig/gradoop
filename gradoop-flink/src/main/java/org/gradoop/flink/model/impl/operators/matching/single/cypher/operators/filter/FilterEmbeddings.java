
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterEmbedding;

/**
 * Filters a set of Embeddings by the given predicates
 * The resulting embeddings have the same schema as the input embeddings
 */
public class FilterEmbeddings implements PhysicalOperator {
  /**
   * Candidate Embeddings
   */
  private final DataSet<Embedding> input;
  /**
   * Predicates in conjunctive normal form
   */
  private CNF predicates;
  /**
   * Maps variable names to embedding entries;
   */
  private final EmbeddingMetaData metaData;

  /**
   * Operator name used for Flink operator description
   */
  private String name;

  /**
   * New embedding filter operator
   * @param input Candidate embeddings
   * @param predicates Predicates to used for filtering
   * @param metaData Maps variable names to embedding entries
   */
  public FilterEmbeddings(DataSet<Embedding> input, CNF predicates,
    EmbeddingMetaData metaData) {
    this.input = input;
    this.predicates = predicates;
    this.metaData = metaData;
    this.setName("FilterEmbeddings");
  }

  /**
   * {@inheritDoc}
   */
  public DataSet<Embedding> evaluate() {
    return input
      .filter(new FilterEmbedding(predicates, metaData))
      .name(getName());
  }

  @Override
  public void setName(String newName) {
    this.name = newName;
  }

  @Override
  public String getName() {
    return this.name;
  }
}
