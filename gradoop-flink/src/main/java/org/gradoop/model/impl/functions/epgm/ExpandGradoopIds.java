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
package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * (id1,{id2,id3}) => (id1,id2),(id1,id3)
 */
public class ExpandGradoopIds implements FlatMapFunction
  <Tuple2<GradoopId, GradoopIdSet>, Tuple2<GradoopId, GradoopId>> {

  @Override
  public void flatMap(
    Tuple2<GradoopId, GradoopIdSet> pair,
    Collector<Tuple2<GradoopId, GradoopId>> collector) throws Exception {

    GradoopId fromId = pair.f0;

    for (GradoopId toId : pair.f1) {
      collector.collect(new Tuple2<>(fromId, toId));
    }

  }
}
