package org.gradoop.flink.model.impl.operators.join.blocks;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

import java.io.Serializable;

/**
 * Created by vasistas on 14/02/17.
 */
public class KeySelectorFromVertexSrcOrDest implements KeySelector<Edge, GradoopId>, Serializable {

  public final boolean isLeft;

  public KeySelectorFromVertexSrcOrDest(boolean elem) {
    isLeft = elem;
  }

  @Override
  public GradoopId getKey(Edge e1) throws Exception {
    return isLeft ? e1.getSourceId() : e1.getTargetId();
  }
}
