/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

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
