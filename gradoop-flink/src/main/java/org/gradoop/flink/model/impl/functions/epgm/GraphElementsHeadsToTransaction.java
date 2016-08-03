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

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
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
