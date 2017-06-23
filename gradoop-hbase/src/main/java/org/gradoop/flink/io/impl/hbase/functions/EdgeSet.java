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

package org.gradoop.flink.io.impl.hbase.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Takes grouped edges as input and outputs a tuple containing either source or
 * target vertex id and the edges.
 *
 * edge+ -> ([sourceId|targetId], edge+)
 *
 * @param <E> EPGM edge type
 */
public abstract class EdgeSet<E extends EPGMEdge>
  implements GroupReduceFunction<E, Tuple2<GradoopId, Set<E>>> {

  /**
   * True, if edges are grouped by source id
   * False, if edges are grouped by target id
   */
  protected boolean extractBySourceId = true;

  @Override
  public void reduce(Iterable<E> iterable,
    Collector<Tuple2<GradoopId, Set<E>>> collector) throws Exception {
    Iterator<E> edgeIt = iterable.iterator();
    E edge = edgeIt.next();
    GradoopId vId = extractBySourceId ? edge.getSourceId() : edge.getTargetId();

    Set<E> edgeSet = new HashSet<>();
    edgeSet.add(edge);
    while (edgeIt.hasNext()) {
      edgeSet.add(edgeIt.next());
    }

    collector.collect(new Tuple2<>(vId, edgeSet));
  }
}
