package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions
  .FilterEmbeddingElements;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.List;

/**
 * Filters {@link Embedding} objects resulting from a pattern matching based on retrun pattern
 * variables existing in pattern matching query. The out put is a {@link Embedding} representation
 * of a join between pattern matching and return pattern query variables.
 */
public class FilterEmbeddingsElements implements PhysicalOperator {
  /**
   * Input graph elements
   */
  private final DataSet<Embedding> input;
  /**
   * Return pattern variables that already exist in pattern matching query
   */
  private final List<String> existingReturnPatternVariables;
  /**
   * Meta data for pattern matching embeddings
   */
  private final EmbeddingMetaData embeddingMetaData;
  /**
   * Meta data for return pattern embeddings
   */
  private final EmbeddingMetaData newMetaData;
  /**
   * Operator name used for Flink operator description
   */
  private String name;

  /**
   * New embeddings filter operator
   *
   * @param input                          Candidate embeddings
   * @param existingReturnPatternVariables Join of query and return pattern variables
   * @param embeddingMetaData              Pattern matching meta data
   * @param newMetaData                    Return pattern meta data
   */
  public FilterEmbeddingsElements(DataSet<Embedding> input,
    List<String> existingReturnPatternVariables, EmbeddingMetaData embeddingMetaData,
    EmbeddingMetaData newMetaData) {
    this.input = input;
    this.existingReturnPatternVariables = existingReturnPatternVariables;
    this.embeddingMetaData = embeddingMetaData;
    this.newMetaData = newMetaData;
    this.setName("FilterEmbeddingsElements");
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input.map(
      new FilterEmbeddingElements(existingReturnPatternVariables, embeddingMetaData, newMetaData));
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
