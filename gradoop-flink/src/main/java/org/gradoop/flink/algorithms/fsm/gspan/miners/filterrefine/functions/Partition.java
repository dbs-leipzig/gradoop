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
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;

import java.util.Collection;

/**
 * graph,.. => (workerId, {graph,..})
 */
public class Partition extends RichMapPartitionFunction
  <GSpanGraph, Tuple2<Integer, Collection<GSpanGraph>>> {

  @Override
  public void mapPartition(Iterable<GSpanGraph> iterable,
    Collector<Tuple2<Integer, Collection<GSpanGraph>>> collector) throws
    Exception {

    int workerId = getRuntimeContext().getIndexOfThisSubtask();
    Collection<GSpanGraph> transactions = Lists.newArrayList(iterable);

    collector.collect(new Tuple2<>(workerId, transactions));
  }
}
