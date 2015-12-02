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

package org.gradoop.model.impl.operators.difference;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.operators.base.SetOperatorBase;

import java.util.Iterator;

/**
 * Returns a collection with all logical graphs that are contained in the
 * first input collection but not in the second.
 * Graph equality is based on their respective identifiers.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @see DifferenceBroadcast
 */
public class Difference<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  extends SetOperatorBase<G, V, E> {

  /**
   * Computes the subgraph dataset for the resulting collection.
   *
   * @return subgraph dataset of the resulting collection
   */
  @Override
  protected DataSet<G> computeNewGraphHeads() {
    // assign 1L to each subgraph in the first collection
    DataSet<Tuple2<G, Long>> thisGraphs = firstCollection.getGraphHeads()
      .map(new Tuple2LongMapper<G>(1L));
    // assign 2L to each subgraph in the second collection
    DataSet<Tuple2<G, Long>> otherGraphs = secondCollection.getGraphHeads()
      .map(new Tuple2LongMapper<G>(2L));

    // union the subgraphs, group them by their identifier and check that
    // there is no graph in the group that belongs to the second collection
    return thisGraphs.union(otherGraphs)
      .groupBy(new SubgraphTupleKeySelector<G, Long>()).reduceGroup(
        new GroupReduceFunction<Tuple2<G, Long>, G>() {
          @Override
          public void reduce(
            Iterable<Tuple2<G, Long>> iterable,
            Collector<G> collector) throws Exception {
            Iterator<Tuple2<G, Long>> it = iterable.iterator();
            Tuple2<G, Long> graphHeadWithLong = null;
            boolean inOtherCollection = false;
            while (it.hasNext()) {
              graphHeadWithLong = it.next();
              if (graphHeadWithLong.f1.equals(2L)) {
                // graph head is in second collection
                inOtherCollection = true;
                break;
              }
            }
            if (!inOtherCollection && graphHeadWithLong != null) {
              collector.collect(graphHeadWithLong.f0);
            }
          }
        });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Difference.class.getName();
  }
}
