package org.gradoop.flink.model.impl.operators.matching.single.cypher.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;

import java.util.List;

/**
 * Computes a combines hash value from given columns.
 */
public class ExtractJoinColumns implements KeySelector<Embedding, Integer> {

  /**
   * Columns to create hash code from.
   */
  private final List<Integer> columns;

  /**
   * Creates the key selector
   *
   * @param columns columns to create hash code from
   */
  public ExtractJoinColumns(List<Integer> columns) {
    this.columns = columns;
  }

  @Override
  public Integer getKey(Embedding embedding) throws Exception {
    int hashCode = 17;
    for (int column : columns) {
      hashCode = 31 * hashCode + embedding.getEntry(column).getId().hashCode();
    }
    return hashCode;
  }
}
