/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Iterator;
import java.util.Set;

/**
 * Generates a graphTransaction for a set of vertices, edges and a graph head
 * based on the same graph.
 *
 * Read fields first:
 *
 * f1: graph element
 */
@FunctionAnnotation.ReadFieldsFirst("f1")
public class GraphElementsHeadsToTransaction implements CoGroupFunction
  <Tuple2<GradoopId, GraphElement>, GraphHead, GraphTransaction> {

  @Override
  public void coGroup(
    Iterable<Tuple2<GradoopId, GraphElement>> graphElements,
    Iterable<GraphHead> graphHeads,
    Collector<GraphTransaction> out) throws Exception {

    Iterator<GraphHead> graphHeadIter = graphHeads.iterator();

    if (graphHeadIter.hasNext()) {
      Set<Vertex> vertices = Sets.newHashSet();
      Set<Edge> edges = Sets.newHashSet();
      GraphHead graphHead = graphHeadIter.next();

      for (Tuple2<GradoopId, GraphElement> graphElement : graphElements) {

        GraphElement el = graphElement.f1;
        if (el instanceof Vertex) {
          vertices.add((Vertex) el);
        } else if (el instanceof Edge) {
          edges.add((Edge) el);
        }
      }

      out.collect(new GraphTransaction(graphHead, vertices, edges));
    }
  }
}
