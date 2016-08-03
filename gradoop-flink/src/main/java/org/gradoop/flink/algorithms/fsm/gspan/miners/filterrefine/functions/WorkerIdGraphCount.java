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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;

import java.util.Collection;

/**
 * (workerId, {graph,..}) => (workerId, |{graph,..}|)
 */
public class WorkerIdGraphCount implements MapFunction
  <Tuple2<Integer, Collection<GSpanGraph>>, Tuple2<Integer, Integer>> {

  @Override
  public Tuple2<Integer, Integer> map(
    Tuple2<Integer, Collection<GSpanGraph>> workerIdGraphs) throws Exception {
    return new Tuple2<>(workerIdGraphs.f0, workerIdGraphs.f1.size());
  }
}
