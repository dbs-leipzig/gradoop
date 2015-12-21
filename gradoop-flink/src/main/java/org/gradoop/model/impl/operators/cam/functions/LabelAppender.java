package org.gradoop.model.impl.operators.cam.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.api.EPGMLabeled;
import org.gradoop.model.impl.operators.cam.tuples.VertexLabel;

public class LabelAppender<L extends EPGMLabeled> implements
  JoinFunction<L, L, L> {

  @Override
  public L join(L left, L right) throws Exception {

    String rightLabel = right == null ? "" : right.getLabel();

    left.setLabel(left.getLabel() + rightLabel);

    return left;
  }
}
