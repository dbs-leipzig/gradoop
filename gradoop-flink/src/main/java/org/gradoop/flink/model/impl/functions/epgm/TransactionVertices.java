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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Set;

/**
 * (graphHead, {vertex,..}, {edge,..}) => vertex,..
 */
public class TransactionVertices implements
  FlatMapFunction<Tuple3<GraphHead, Set<Vertex>, Set<Edge>>, Vertex> {

  @Override
  public void flatMap(Tuple3<GraphHead, Set<Vertex>, Set<Edge>> graphTriple,
    Collector<Vertex> collector) throws Exception {

    for (Vertex vertex : graphTriple.f1) {
      collector.collect(vertex);
    }
  }

}
