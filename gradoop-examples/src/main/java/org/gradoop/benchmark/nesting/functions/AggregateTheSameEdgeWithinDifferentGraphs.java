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
package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Created by vasistas on 08/04/17.
 */
public class AggregateTheSameEdgeWithinDifferentGraphs<K extends Comparable<K>> implements
  GroupCombineFunction<Tuple6<K, GradoopId, K, String, Properties, GradoopId>,
    Tuple6<K, GradoopId, K, String, Properties, GradoopIdList>> {

  /**
   * Reusable element
   */
  private final Tuple6<K, GradoopId, K, String, Properties, GradoopIdList> reusable;

  /**
   * Default constructor
   */
  public AggregateTheSameEdgeWithinDifferentGraphs() {
    reusable = new Tuple6<>();
    reusable.f5 = new GradoopIdList();
  }

  @Override
  public void combine(
    Iterable<Tuple6<K, GradoopId, K, String, Properties, GradoopId>> values,
    Collector<Tuple6<K, GradoopId, K, String, Properties, GradoopIdList>> out) throws
    Exception {
    reusable.f5.clear();
    K key = null;
    for (Tuple6<K, GradoopId, K, String, Properties, GradoopId> x : values) {
      if (key == null) {
        key = x.f0;
      }
      reusable.f0 = x.f0;
      reusable.f1 = x.f1;
      reusable.f2 = x.f2;
      reusable.f3 = x.f3;
      reusable.f4 = x.f4;
      if (!reusable.f5.contains(x.f5)) {
        reusable.f5.add(x.f5);
      }
    }
    out.collect(reusable);
  }
}
