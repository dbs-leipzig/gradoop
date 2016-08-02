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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Takes an object of type GraphElement, and creates a tuple2 for each
 *  gradoop id containing in the set of the object and the object.
 * element => (graphId, element)
 * @param <EL> graph element type
 */
public class GraphElementExpander<EL extends GraphElement> implements
  FlatMapFunction<EL, Tuple2<GradoopId, GraphElement>> {

  /**
   * reuse tuple
   */
  private  Tuple2<GradoopId, GraphElement> reuse;

  /**
   * constructor
   */
  public GraphElementExpander() {
    reuse = new Tuple2<>();
  }

  @Override
  public void flatMap(EL el, Collector
    <Tuple2<GradoopId, GraphElement>> collector) throws Exception {
    for (GradoopId graphId : el.getGraphIds()) {
      reuse.f0 = graphId;
      reuse.f1 = el;
      collector.collect(reuse);
    }
  }
}
