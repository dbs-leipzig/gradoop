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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.HashSet;
import java.util.Set;

/**
 * {@code (graphHead) =|><| (graphId,{vertex,..},{edge,..}) => (graphHead,{vertex,..},{edge,..})}
 * <p>
 * Forwarded fields first:
 * <br>
 * f0: graph head
 * <p>
 * Forwarded fields second:
 * <br>
 * f1: vertex set<br>
 * f2: edge set
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1;f2")
public class TransactionFromSets implements
  JoinFunction<EPGMGraphHead, Tuple3<GradoopId, Set<EPGMVertex>, Set<EPGMEdge>>, GraphTransaction> {
  /**
   * Used if right hand side is empty
   */
  private static final Set<EPGMVertex> EMPTY_VERTEX_SET = new HashSet<>(0);
  /**
   * Used if right hand side is empty
   */
  private static final Set<EPGMEdge> EMPTY_EDGE_SET = new HashSet<>(0);
  /**
   * Reduce object instantiations
   */
  private final GraphTransaction reuseTransaction = new GraphTransaction();

  @Override
  public GraphTransaction join(EPGMGraphHead graphHead, Tuple3<GradoopId, Set<EPGMVertex>,
    Set<EPGMEdge>> sets) throws Exception {

    reuseTransaction.setGraphHead(graphHead);
    reuseTransaction.setVertices(sets == null ? EMPTY_VERTEX_SET : sets.f1);
    reuseTransaction.setEdges(sets == null ? EMPTY_EDGE_SET : sets.f2);

    return reuseTransaction;
  }
}
