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

package org.gradoop.io.impl.tlf.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.LinkedList;
import java.util.List;

/**
 * Creates a list containing all edge labels.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GraphTransactionToTLFDictionaryEdgeMap<G extends EPGMGraphHead, V
  extends EPGMVertex, E extends EPGMEdge> implements
  FlatMapFunction<GraphTransaction<G, V, E>, List<String>> {

  @Override
  public void flatMap(
    GraphTransaction<G, V, E> graphTransaction, Collector<List<String>>
    collector) throws Exception {
    List<String> list = new LinkedList<>();
    for (E edge : graphTransaction.getEdges()) {
      list.add(edge.getLabel());
    }
    collector.collect(list);
  }
}
