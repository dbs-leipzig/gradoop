
package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * combines string representations of multiple (parallel) edges
 */
public class MultiEdgeStringCombiner implements
  GroupReduceFunction<EdgeString, EdgeString> {

  @Override
  public void reduce(Iterable<EdgeString> iterable,
    Collector<EdgeString> collector) throws Exception {

    List<String> labels = new ArrayList<>();
    Boolean first = true;
    EdgeString combinedLabel = null;

    for (EdgeString edgeString : iterable) {
      if (first) {
        combinedLabel = edgeString;
        first = false;
      }

      labels.add(edgeString.getEdgeLabel());
    }

    Collections.sort(labels);

    if (combinedLabel != null) {
      combinedLabel.setEdgeLabel(StringUtils.join(labels, "&"));
    }

    collector.collect(combinedLabel);
  }
}
