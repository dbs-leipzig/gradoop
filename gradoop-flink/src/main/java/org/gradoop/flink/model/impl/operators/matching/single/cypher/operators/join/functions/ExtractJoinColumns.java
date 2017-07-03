
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

import java.util.List;

/**
 * Given a set of columns, this key selector returns a concatenated string containing the
 * identifiers of the specified columns.
 *
 * (id0,id1,...,idn),[0,2] -> "id0id2"
 */
public class ExtractJoinColumns implements KeySelector<Embedding, String> {
  /**
   * Columns to concatenate ids from
   */
  private final List<Integer> columns;
  /**
   * Stores the concatenated id string
   */
  private final StringBuilder sb;

  /**
   * Creates the key selector
   *
   * @param columns columns to create hash code from
   */
  public ExtractJoinColumns(List<Integer> columns) {
    this.columns = columns;
    this.sb = new StringBuilder();
  }

  @Override
  public String getKey(Embedding value) throws Exception {
    sb.delete(0, sb.length());
    for (Integer column : columns) {
      sb.append(ArrayUtils.toString(value.getRawId(column)));
    }
    return sb.toString();
  }
}
