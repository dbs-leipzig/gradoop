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

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Set;

/**
 * (graphId, element),.. => (graphId, {element,..})
 * @param <EL> graph element type
 */
public class GraphElementSet<EL extends GraphElement> implements
  GroupReduceFunction<Tuple2<GradoopId, EL>, Tuple2<GradoopId, Set<EL>>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, EL>> iterable,
    Collector<Tuple2<GradoopId, Set<EL>>> collector) throws Exception {

    boolean first = true;
    GradoopId graphId = null;

    Set<EL> elements = Sets.newHashSet();

    for (Tuple2<GradoopId, EL> elementPair : iterable) {

      if (first) {
        graphId = elementPair.f0;
        first = false;
      }

      elements.add(elementPair.f1);
    }

    collector.collect(new Tuple2<>(graphId, elements));
  }
}
