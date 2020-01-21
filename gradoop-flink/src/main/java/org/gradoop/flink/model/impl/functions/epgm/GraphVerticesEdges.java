/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphElement;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * combiner: {@code (graphId, [vertex|edges] -> (graphId, {vertex,...}, {edge,...})}<br>
 * reducer: {@code (graphId, {vertex,...}, {edge,...}) -> (graphId, {vertex,...}, {edge,...})}
 * <p>
 * Forwarded fields:
 * <br>
 * {@code f0}: {@code graphId}
 */
@FunctionAnnotation.ForwardedFields("f0")
public class GraphVerticesEdges implements
  GroupCombineFunction<
    Tuple2<GradoopId, EPGMGraphElement>,
    Tuple3<GradoopId, Set<EPGMVertex>, Set<EPGMEdge>>>,
  GroupReduceFunction<
    Tuple3<GradoopId, Set<EPGMVertex>, Set<EPGMEdge>>,
    Tuple3<GradoopId, Set<EPGMVertex>, Set<EPGMEdge>>> {

  /**
   * Creates vertex and edge sets for each transaction.
   *
   * @param values vertices and edges associated to the same graph
   * @param out collector
   * @throws Exception The function may throw Exceptions, which will cause the program to cancel,
   *                   and may trigger the recovery logic.
   */
  @Override
  public void combine(Iterable<Tuple2<GradoopId, EPGMGraphElement>> values,
    Collector<Tuple3<GradoopId, Set<EPGMVertex>, Set<EPGMEdge>>> out) throws Exception {

    Iterator<Tuple2<GradoopId, EPGMGraphElement>> iterator = values.iterator();

    GradoopId graphId    = null;
    Set<EPGMVertex> vertices = new HashSet<>();
    Set<EPGMEdge> edges      = new HashSet<>();

    while (iterator.hasNext()) {
      Tuple2<GradoopId, EPGMGraphElement> next = iterator.next();
      graphId = next.f0;

      EPGMGraphElement element = next.f1;
      if (element instanceof EPGMVertex) {
        vertices.add((EPGMVertex) element);
      } else {
        edges.add((EPGMEdge) element);
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
  public void reduce(Iterable<Tuple3<GradoopId, Set<EPGMVertex>, Set<EPGMEdge>>> values,
    Collector<Tuple3<GradoopId, Set<EPGMVertex>, Set<EPGMEdge>>> out) throws Exception {

    Iterator<Tuple3<GradoopId, Set<EPGMVertex>, Set<EPGMEdge>>> iterator = values.iterator();
    Tuple3<GradoopId, Set<EPGMVertex>, Set<EPGMEdge>> first = iterator.next();

    GradoopId graphId    = first.f0;
    Set<EPGMVertex> vertices = first.f1;
    Set<EPGMEdge> edges      = first.f2;

    while (iterator.hasNext()) {
      Tuple3<GradoopId, Set<EPGMVertex>, Set<EPGMEdge>> next = iterator.next();
      vertices.addAll(next.f1);
      edges.addAll(next.f2);
    }

    out.collect(Tuple3.of(graphId, vertices, edges));
  }
}
