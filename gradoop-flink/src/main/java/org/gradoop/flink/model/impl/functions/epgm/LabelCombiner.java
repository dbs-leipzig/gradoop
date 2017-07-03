
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.api.entities.EPGMLabeled;

/**
 * concatenates the labels of labeled things
 * @param <L> labeled type
 */
public class LabelCombiner<L extends EPGMLabeled> implements
  JoinFunction<L, L, L> {

  @Override
  public L join(L left, L right) throws Exception {

    String rightLabel = right == null ? "" : right.getLabel();

    left.setLabel(left.getLabel() + rightLabel);

    return left;
  }
}
