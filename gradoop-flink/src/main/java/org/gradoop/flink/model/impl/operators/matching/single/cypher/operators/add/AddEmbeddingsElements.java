package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.add;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.add.functions
  .AddEmbeddingElements;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.List;

/**
 * Adds {@link Embedding} entries to a given {@link Embedding} based on a {@link List}
 * of variables via an {@link EmbeddingMetaData} object.
 */
public class AddEmbeddingsElements implements PhysicalOperator {
  /**
   * Input embeddings
   */
  private final DataSet<Embedding> input;
  /**
   * Return pattern variables that do not exist in pattern matching query
   */
  private final List<String> newReturnPatternVariables;
  /**
   * Operator name used for Flink operator description
   */
  private String name;

  /**
   * New embeddings add operator
   *
   * @param input                     Candidate embeddings
   * @param newReturnPatternVariables Difference between query and return pattern variables
   */
  public AddEmbeddingsElements(DataSet<Embedding> input, List<String> newReturnPatternVariables) {
    this.input = input;
    this.newReturnPatternVariables = newReturnPatternVariables;
    this.setName("AddEmbeddingsElements");
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input.map(new AddEmbeddingElements(newReturnPatternVariables));
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
