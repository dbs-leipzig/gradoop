
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.EPGMLabeled;

/**
 * Accepts all elements which have the same label as specified.
 *
 * @param <L> EPGM labeled type
 */
public class ByLabel<L extends EPGMLabeled> implements FilterFunction<L> {
  /**
   * Label to be filtered on.
   */
  private String label;

  /**
   * Valued constructor.
   *
   * @param label label to be filtered on
   */
  public ByLabel(String label) {
    this.label = label;
  }

  @Override
  public boolean filter(L l) throws Exception {
    return l.getLabel().equals(label);
  }
}
