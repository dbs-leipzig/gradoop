
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

/**
 * Extracts a join key from an id stored in an embedding record
 * The id is referenced via its column index.
 */
public class ExtractExpandColumn implements KeySelector<Embedding, GradoopId> {
  /**
   * Column that holds the id which will be used as key
   */
  private final Integer column;

  /**
   * Creates the key selector
   *
   * @param column column that holds the id which will be used as key
   */
  public ExtractExpandColumn(Integer column) {
    this.column = column;
  }

  @Override
  public GradoopId getKey(Embedding value) throws Exception {
    return value.getId(column);
  }
}
