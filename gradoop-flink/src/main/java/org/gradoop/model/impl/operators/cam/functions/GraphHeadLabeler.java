package org.gradoop.model.impl.operators.cam.functions;


import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.operators.cam.tuples.GraphHeadLabel;

public interface GraphHeadLabeler<G extends EPGMGraphHead>
  extends MapFunction<G, GraphHeadLabel> {
}
