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

package org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.tuples.RefinementMessage;

import java.util.Collection;
import java.util.Iterator;

/**
 * RefinementMessage,.. => (workerId, {Subgraph,..})
 */
public class RefinementCalls implements GroupReduceFunction
  <RefinementMessage, Tuple2<Integer, Collection<CompressedDFSCode>>> {

  @Override
  public void reduce(
    Iterable<RefinementMessage> iterable,
    Collector<Tuple2<Integer, Collection<CompressedDFSCode>>> collector) throws
    Exception {

    Iterator<RefinementMessage> iterator = iterable.iterator();

    RefinementMessage message = iterator.next();

    int workerId = message.getWorkerId();

    Collection<CompressedDFSCode> codes = Lists
      .newArrayList(message.getSubgraph());

    while (iterator.hasNext()) {
      message = iterator.next();
      codes.add(message.getSubgraph());
    }
    collector.collect(new Tuple2<>(workerId, codes));

  }

}
