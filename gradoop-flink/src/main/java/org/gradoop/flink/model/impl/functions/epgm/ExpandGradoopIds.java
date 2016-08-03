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
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Takes a tuple 2, containing an object and a gradoop id set, and creates one
 * new tuple 2 of the object and a gradoop id for each gradoop id in the set.
 *
 * @param <T> f0 type
 */
@FunctionAnnotation.ReadFields("f1")
@FunctionAnnotation.ForwardedFields("f0->f0")
public class ExpandGradoopIds<T> implements FlatMapFunction
  <Tuple2<T, GradoopIdSet>, Tuple2<T, GradoopId>> {

  @Override
  public void flatMap(
    Tuple2<T, GradoopIdSet> pair,
    Collector<Tuple2<T, GradoopId>> collector) throws Exception {

    T firstField = pair.f0;

    for (GradoopId toId : pair.f1) {
      collector.collect(new Tuple2<>(firstField, toId));
    }

  }
}
