
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.algorithms.fsm.transactional.tle.interestingness.Interestingness;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraph;

import java.util.Collection;
import java.util.Map;

/**
 * Evaluates subgraphs of different categories with regard to category
 * frequency and global interestingness.
 */
public class CategoryFrequentAndInteresting
  extends RichGroupReduceFunction<CCSSubgraph, CCSSubgraph> {

  /**
   * Interestingness measure
   */
  private final Interestingness interestingness;

  /**
   * minimum frequency per category
   */
  private Map<String, Long> categoryMinFrequencies;

  /**
   * graph count per category
   */
  private Map<String, Long> categoryCounts;

  /**
   * Constructor.
   *
   * @param minInterestingness interestingness threshold
   */
  public CategoryFrequentAndInteresting(float minInterestingness) {
    this.interestingness = new Interestingness(minInterestingness);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.categoryMinFrequencies = getRuntimeContext()
      .<Map<String, Long>>getBroadcastVariable(DIMSpanConstants.MIN_FREQUENCY).get(0);

    this.categoryCounts = getRuntimeContext()
      .<Map<String, Long>>getBroadcastVariable(TFSMConstants.GRAPH_COUNT).get(0);
  }

  @Override
  public void reduce(
    Iterable<CCSSubgraph> values, Collector<CCSSubgraph> out) throws Exception {

    Collection<CCSSubgraph> subgraphs = Lists.newArrayList();

    boolean onceFrequent = false;

    Map<String, Float> categorySupports = Maps.newHashMap();

    float avgSupport = 0.0f;

    for (CCSSubgraph subgraph : values) {
      subgraphs.add(subgraph);

      String category = subgraph.getCategory();
      long categoryFrequency = subgraph.getCount();

      float categorySupport =
        (float) categoryFrequency / categoryCounts.get(category);

      avgSupport += categorySupport;

      categorySupports.put(category, categorySupport);

      if (categoryFrequency >= categoryMinFrequencies.get(category) &&
        !onceFrequent) {
        onceFrequent = true;
      }
    }

    if (onceFrequent) {
      avgSupport /= categoryMinFrequencies.size();

      for (CCSSubgraph subgraph : subgraphs) {
        float categorySupport = categorySupports.get(subgraph.getCategory());

        subgraph.setInteresting(
          interestingness.isInteresting(categorySupport, avgSupport));

        out.collect(subgraph);
      }
    }
  }
}
