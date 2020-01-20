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
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.LeftSideWithRightGraphs;

/**
 * Verifies the edge set for each graph in a collection, removing dangling edges, i.e. edges with a
 * source- or target-id not matching any vertices of this graph.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The graph type.
 * @param <GC> The graph collection type.
 */
public class VerifyCollection<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements UnaryBaseGraphCollectionToBaseGraphCollectionOperator<GC> {

  @Override
  public GC execute(GC collection) {
    DataSet<V> vertices = collection.getVertices();
    DataSet<E> verifiedEdges = collection.getEdges()
      .join(vertices)
      .where(new SourceId<>())
      .equalTo(new Id<>())
      .with(new LeftSideWithRightGraphs<>())
      .name("Verify Edges (1/2)")
      .join(vertices)
      .where(new TargetId<>())
      .equalTo(new Id<>())
      .with(new LeftSideWithRightGraphs<>())
      .name("Verify Edges (2/2)");
    return collection.getFactory().fromDataSets(collection.getGraphHeads(), vertices, verifiedEdges);
  }
}
