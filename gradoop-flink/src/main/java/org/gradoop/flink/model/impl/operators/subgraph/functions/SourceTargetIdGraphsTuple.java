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

package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * For each edge, collect two tuple 2 containing its source or target id in the
 * first field and all the graphs this edge is contained in in its second field.
 *
 * @param <E> epgm edge type
 */

@FunctionAnnotation.ReadFields("sourceId;targetId")
@FunctionAnnotation.ForwardedFields("graphIds->f1")
public class SourceTargetIdGraphsTuple<E extends Edge>
  implements FlatMapFunction<E, Tuple2<GradoopId, GradoopIdSet>> {

  @Override
  public void flatMap(
    E e,
    Collector<Tuple2<GradoopId, GradoopIdSet>> collector) throws
    Exception {

    collector.collect(new Tuple2<>(e.getSourceId(), e.getGraphIds()));
    collector.collect(new Tuple2<>(e.getTargetId(), e.getGraphIds()));
  }
}
