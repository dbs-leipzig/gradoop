package org.gradoop.model.impl.operators.cam.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.operators.cam.tuples.GraphHeadLabel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CollectionLabel
  implements GroupReduceFunction<GraphHeadLabel, String> {

  @Override
  public void reduce(Iterable<GraphHeadLabel> graphHeadLabels,
    Collector<String> collector) throws Exception {

    List<String> graphLabels = new ArrayList<>();

    for(GraphHeadLabel graphHeadLabel : graphHeadLabels) {
      String graphLabel = graphHeadLabel.getLabel();
      if (! graphLabel.equals("")) {
        graphLabels.add(graphLabel);
      }
    }

    Collections.sort(graphLabels);

    collector.collect(StringUtils.join(graphLabels,"\n"));

//    System.out.println("-----");
//    System.out.println(StringUtils.join(graphLabels,"\n"));
  }
}
