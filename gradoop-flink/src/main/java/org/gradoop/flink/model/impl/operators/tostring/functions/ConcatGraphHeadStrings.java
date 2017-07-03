
package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * concatenates the sorted string representations of graph heads to represent a
 * collection
 */
public class ConcatGraphHeadStrings
  implements GroupReduceFunction<GraphHeadString, String> {

  @Override
  public void reduce(Iterable<GraphHeadString> graphHeadLabels,
    Collector<String> collector) throws Exception {

    List<String> graphLabels = new ArrayList<>();

    for (GraphHeadString graphHeadString : graphHeadLabels) {
      String graphLabel = graphHeadString.getLabel();
      if (! graphLabel.equals("")) {
        graphLabels.add(graphLabel);
      }
    }

    Collections.sort(graphLabels);

    collector.collect(StringUtils.join(graphLabels, "\n"));
  }
}
