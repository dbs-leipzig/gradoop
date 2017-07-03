/**
 * Copyright © 2014 Gradoop (University of Leipzig - Database Research Group)
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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.common.model.impl.pojo.GraphHead;

import java.util.Set;

/**
 * graphTransaction <=> (graphHead, {vertex,..}, {edge, ..})
 *
 * Forwarded fields:
 *
 * f0: transaction graph head
 * f1: transaction vertices
 * f2: transaction edges
 */
@FunctionAnnotation.ForwardedFields("f0;f1;f2")
public class GraphTransactionTriple
  implements MapFunction<GraphTransaction, Tuple3<GraphHead, Set<Vertex>, Set<Edge>>> {
  /**
   * Reduce object instantiations
   */
  private final Tuple3<GraphHead, Set<Vertex>, Set<Edge>> reuseTuple = new Tuple3<>();

  @Override
  public Tuple3<GraphHead, Set<Vertex>, Set<Edge>> map(GraphTransaction transaction)
    throws Exception {

    reuseTuple.f0 = transaction.getGraphHead();
    reuseTuple.f1 = transaction.getVertices();
    reuseTuple.f2 = transaction.getEdges();

    return reuseTuple;
  }
}
