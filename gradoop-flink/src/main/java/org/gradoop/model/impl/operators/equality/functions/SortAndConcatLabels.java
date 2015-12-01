package org.gradoop.model.impl.operators.equality.functions;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMLabeled;

import java.util.Collections;
import java.util.List;

/**
 * "Z", "A", "M" => "AMZ"
 *
 * @param <L> labeled type
 */
public class SortAndConcatLabels<L extends EPGMLabeled>
  implements GroupReduceFunction<L, L> {
  @Override
  public void reduce(Iterable<L> iterable,
    Collector<L> collector) throws Exception {

    List<L> fatLabels = Lists.newArrayList(iterable);

    if (!fatLabels.isEmpty()) {
      List<String> labels = Lists.newArrayList();

      for (L fatLabel : fatLabels) {
        labels.add(fatLabel.getLabel());
      }

      Collections.sort(labels);

      L concatenatedFatLabel = fatLabels.get(0);
      concatenatedFatLabel.setLabel(StringUtils.join(labels, ","));

      collector.collect(concatenatedFatLabel);
    }
  }
}
