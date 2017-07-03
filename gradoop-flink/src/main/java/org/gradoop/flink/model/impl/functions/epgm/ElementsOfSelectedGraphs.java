
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;

import java.util.Collection;

/**
 * graphIds (BC)
 * element => (graphId, element),..
 *
 * \forall (graphId, element) : graphId \in graphIds
 *
 * @param <EL> graph element type
 */
public class ElementsOfSelectedGraphs<EL extends GraphElement> extends
  RichFlatMapFunction<EL, Tuple2<GradoopId, EL>> {

  /**
   * constant string for "graph ids"
   */
  public static final String GRAPH_IDS = "graphIds";

  /**
   * graph ids
   */
  protected Collection<GradoopId> graphIds;

  /**
   * reuse tuple
   */
  private Tuple2<GradoopId, EL> reuse;

  /**
   * constructor
   */
  public ElementsOfSelectedGraphs() {
    reuse = new Tuple2<>();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    graphIds = getRuntimeContext().getBroadcastVariable(GRAPH_IDS);
  }

  @Override
  public void flatMap(EL el, Collector
    <Tuple2<GradoopId, EL>> collector) throws Exception {
    for (GradoopId graphId : el.getGraphIds()) {
      if (graphIds.contains(graphId)) {
        reuse.f0 = graphId;
        reuse.f1 = el;
        collector.collect(reuse);
      }
    }
  }
}
