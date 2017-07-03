
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.CCSGraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CategoryCountableLabel;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;

import java.util.Set;

/**
 * graph => (category, label, 1)
 */
public class CategoryEdgeLabels implements
  FlatMapFunction<CCSGraph, CategoryCountableLabel> {

  /**
   * reuse tuple to avoid instantiations
   */
  private CategoryCountableLabel reuseTuple =
    new CategoryCountableLabel(null, null, 1L);

  @Override
  public void flatMap(CCSGraph graph,
    Collector<CategoryCountableLabel> out) throws Exception {

    Set<String> edgeLabels =
      Sets.newHashSetWithExpectedSize(graph.getEdges().size());

    for (FSMEdge edge : graph.getEdges().values()) {
      edgeLabels.add(edge.getLabel());
    }

    for (String label : edgeLabels) {
      reuseTuple.setCategory(graph.getCategory());
      reuseTuple.setLabel(label);
      out.collect(reuseTuple);
    }
  }
}
