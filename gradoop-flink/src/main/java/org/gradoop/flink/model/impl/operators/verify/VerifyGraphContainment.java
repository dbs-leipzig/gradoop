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
package org.gradoop.flink.model.impl.operators.verify;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.verify.functions.RemoveDanglingGraphIds;

/**
 * Verifies a graphs elements, removing dangling graph ids, i.e. ids different from this graphs id.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The graph type.
 * @param <GC> The graph collection type.
 */
public class VerifyGraphContainment<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements UnaryBaseGraphToBaseGraphOperator<LG> {

  @Override
  public LG execute(LG collection) {
    DataSet<GradoopId> idSet = collection.getGraphHead().map(new Id<>());

    DataSet<V> verifiedVertices = collection.getVertices()
      .map(new RemoveDanglingGraphIds<>())
      .withBroadcastSet(idSet, RemoveDanglingGraphIds.GRAPH_ID_SET);

    DataSet<E> verifiedEdges = collection.getEdges()
      .map(new RemoveDanglingGraphIds<>())
      .withBroadcastSet(idSet, RemoveDanglingGraphIds.GRAPH_ID_SET);

    return collection.getFactory()
      .fromDataSets(collection.getGraphHead(), verifiedVertices, verifiedEdges);
  }
}
