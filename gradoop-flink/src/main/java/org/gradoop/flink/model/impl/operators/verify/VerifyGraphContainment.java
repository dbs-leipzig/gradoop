/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.verify;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.verify.functions.RemoveOtherGraphs;

/**
 * Verifies a graphs elements, removing dangling graph ids, i.e. different from this graphs id.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The graph type.
 */
public class VerifyGraphContainment<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  LG extends BaseGraph<G, V, E, LG>> implements UnaryBaseGraphToBaseGraphOperator<LG> {

  @Override
  public LG execute(LG collection) {
    DataSet<GradoopId> idSet = collection.getGraphHead().map(new Id<>());

    DataSet<V> verifiedVertices = collection.getVertices()
      .map(new RemoveOtherGraphs<>())
      .withBroadcastSet(idSet, RemoveOtherGraphs.GRAPH_ID_SET);

    DataSet<E> verifiedEdges = collection.getEdges()
      .map(new RemoveOtherGraphs<>())
      .withBroadcastSet(idSet, RemoveOtherGraphs.GRAPH_ID_SET);

    return collection.getFactory()
      .fromDataSets(collection.getGraphHead(), verifiedVertices, verifiedEdges);
  }
}
