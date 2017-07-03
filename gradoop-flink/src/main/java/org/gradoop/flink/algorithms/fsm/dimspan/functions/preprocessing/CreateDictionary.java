
package org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.LabelComparator;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.List;

/**
 * (label, frequency),.. => [label,..]
 */
public class CreateDictionary implements GroupReduceFunction<WithCount<String>, String[]> {

  /**
   * comparator used to determine frequency-dependent translation
   */
  private final LabelComparator comparator;

  /**
   * Constructor.
   *
   * @param comparator label comparator
   */
  public CreateDictionary(LabelComparator comparator) {
    this.comparator = comparator;
  }

  @Override
  public void reduce(
    Iterable<WithCount<String>> iterable, Collector<String[]> collector) throws Exception {

    // Sort distinct labels
    List<WithCount<String>> stringsWithCount = Lists.newArrayList(iterable);
    stringsWithCount.sort(comparator);

    // Create dictionary with default label
    List<String> dictionary = Lists.newArrayList();

    for (WithCount<String> stringWithCount : stringsWithCount) {
      dictionary.add(stringWithCount.getObject());
    }

    collector.collect(dictionary.toArray(new String[dictionary.size()]));
  }
}
