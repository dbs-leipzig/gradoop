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

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMGraphElement;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
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
  <Tuple2<GradoopId, EPGMGraphElement>, EPGMGraphHead, GraphTransaction> {

  @Override
  public void coGroup(
    Iterable<Tuple2<GradoopId, EPGMGraphElement>> graphElements,
    Iterable<EPGMGraphHead> graphHeads,
    Collector<GraphTransaction> out) throws Exception {

    Iterator<EPGMGraphHead> graphHeadIter = graphHeads.iterator();

    if (graphHeadIter.hasNext()) {
      Set<EPGMVertex> vertices = Sets.newHashSet();
      Set<EPGMEdge> edges = Sets.newHashSet();
      EPGMGraphHead graphHead = graphHeadIter.next();

      for (Tuple2<GradoopId, EPGMGraphElement> graphElement : graphElements) {

        EPGMGraphElement el = graphElement.f1;
        if (el instanceof EPGMVertex) {
          vertices.add((EPGMVertex) el);
        } else if (el instanceof EPGMEdge) {
          edges.add((EPGMEdge) el);
        }
      }

      out.collect(new GraphTransaction(graphHead, vertices, edges));
    }
  }
}
