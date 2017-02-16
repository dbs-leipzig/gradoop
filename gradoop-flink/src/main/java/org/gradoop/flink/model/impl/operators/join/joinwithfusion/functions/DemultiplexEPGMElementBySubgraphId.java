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

package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;

/**
 * Splits each EPGMElement e into a tuple <t,e> where t is a graphid to where e belongs
 * Created by vasistas on 15/02/17.
 */
public class DemultiplexEPGMElementBySubgraphId<K extends GraphElement> implements FlatMapFunction<K,
  Tuple2<GradoopId,
  K>> {

  /**
   * Reusable field used for speedup
   */
  private final Tuple2<GradoopId, K> reusable;

  /**
   * Default constructor
   */
  public DemultiplexEPGMElementBySubgraphId() {
    reusable = new Tuple2<GradoopId, K>();
  }

  @Override
  public void flatMap(K value, Collector<Tuple2<GradoopId, K>> out) throws Exception {
    for (GradoopId id : value.getGraphIds()) {
      reusable.f0 = id;
      reusable.f1 = value;
      out.collect(reusable);
    }
  }
}
