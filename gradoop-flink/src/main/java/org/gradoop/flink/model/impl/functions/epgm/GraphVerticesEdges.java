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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * combiner: (graphId, [vertex|edges] -> (graphId, {vertex,...}, {edge,...})
 * reducer:  (graphId, {vertex,...}, {edge,...}) -> (graphId, {vertex,...}, {edge,...})
 *
 * Forwarded fields:
 *
 * f0: graph head id
 */
@FunctionAnnotation.ForwardedFields("f0")
public class GraphVerticesEdges implements
  GroupCombineFunction<
    Tuple2<GradoopId, GraphElement>, Tuple3<GradoopId, Set<Vertex>, Set<Edge>>>,
  GroupReduceFunction<
    Tuple3<GradoopId, Set<Vertex>, Set<Edge>>, Tuple3<GradoopId, Set<Vertex>, Set<Edge>>> {

  /**
   * Creates vertex and edge sets for each transaction.
   *
   * @param values vertices and edges associated to the same graph
   * @param out collector
   * @throws Exception The function may throw Exceptions, which will cause the program to cancel,
   *                   and may trigger the recovery logic.
   */
  @Override
  public void combine(Iterable<Tuple2<GradoopId, GraphElement>> values,
    Collector<Tuple3<GradoopId, Set<Vertex>, Set<Edge>>> out) throws Exception {

    Iterator<Tuple2<GradoopId, GraphElement>> iterator = values.iterator();

    GradoopId graphId    = null;
    Set<Vertex> vertices = new HashSet<>();
    Set<Edge> edges      = new HashSet<>();

    while (iterator.hasNext()) {
      Tuple2<GradoopId, GraphElement> next = iterator.next();
      graphId = next.f0;

      GraphElement element = next.f1;
      if (element instanceof Vertex) {
        vertices.add((Vertex) element);
      } else {
        edges.add((Edge) element);
      }
    }

    out.collect(Tuple3.of(graphId, vertices, edges));
  }

  /**
   * Merges sets created by the combiners.
   *
   * @param values partial vertex and edge sets for one transaction
   * @param out collector
   * @throws Exception The function may throw Exceptions, which will cause the program to cancel,
   *                   and may trigger the recovery logic.
   */
  @Override
  public void reduce(Iterable<Tuple3<GradoopId, Set<Vertex>, Set<Edge>>> values,
    Collector<Tuple3<GradoopId, Set<Vertex>, Set<Edge>>> out) throws Exception {

    Iterator<Tuple3<GradoopId, Set<Vertex>, Set<Edge>>> iterator = values.iterator();
    Tuple3<GradoopId, Set<Vertex>, Set<Edge>> first = iterator.next();

    GradoopId graphId    = first.f0;
    Set<Vertex> vertices = first.f1;
    Set<Edge> edges      = first.f2;

    while (iterator.hasNext()) {
      Tuple3<GradoopId, Set<Vertex>, Set<Edge>> next = iterator.next();
      vertices.addAll(next.f1);
      edges.addAll(next.f2);
    }

    out.collect(Tuple3.of(graphId, vertices, edges));
  }
}
