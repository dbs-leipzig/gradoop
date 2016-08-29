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
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Set;

/**
 * (graphHead) |><| (graphId,{vertex,..},{edge,..})
 *    => (graphHead,{vertex,..},{edge,..})
 */
public class TransactionFromSets implements JoinFunction
  <GraphHead, Tuple3<GradoopId, Set<Vertex>, Set<Edge>>, GraphTransaction> {

  @Override
  public GraphTransaction join(GraphHead graphHead,
    Tuple3<GradoopId, Set<Vertex>, Set<Edge>> sets) throws Exception {

    Set<Vertex> vertices;
    Set<Edge> edges;

    if (sets.f0 == null) {
      vertices = Sets.newHashSetWithExpectedSize(0);
      edges = Sets.newHashSetWithExpectedSize(0);
    } else {
      vertices = sets.f1;
      edges = sets.f2;
    }

    return new GraphTransaction(graphHead, vertices, edges);
  }
}
