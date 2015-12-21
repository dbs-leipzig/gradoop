package org.gradoop.model.impl.operators.cam.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.operators.cam.tuples.EdgeLabel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MultiEdgeCombiner implements
  GroupReduceFunction<EdgeLabel, EdgeLabel>{

  @Override
  public void reduce(Iterable<EdgeLabel> iterable,
    Collector<EdgeLabel> collector) throws Exception {

    List<String> labels = new ArrayList<>();
    Boolean first = true;
    EdgeLabel combinedLabel = null;

    for(EdgeLabel edgeLabel : iterable) {
      if(first) {
        combinedLabel = edgeLabel;
        first = false;
      }

      labels.add(edgeLabel.getEdgeLabel());
    }

    Collections.sort(labels);

    if(combinedLabel != null){
      combinedLabel.setEdgeLabel(StringUtils.join(labels, "&"));
    }

    collector.collect(combinedLabel);
  }
}
