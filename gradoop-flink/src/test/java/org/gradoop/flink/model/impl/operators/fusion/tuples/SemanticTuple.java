package org.gradoop.flink.model.impl.operators.fusion.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Created by Giacomo Bergami on 21/02/17.
 *
 * Semantically extending the tuple used for creating stuff
 *
 */
public class SemanticTuple extends Tuple4<GradoopId,GradoopId,GraphHead,Edge> {

  public SemanticTuple() {
  }
  public SemanticTuple(GradoopId sourceId, GradoopId destinationId, GraphHead hypervertexHead,
    Edge hypervertexSubEdge) {
    super(sourceId,destinationId,hypervertexHead,hypervertexSubEdge);
  }

}
