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
package org.gradoop.flink.model.impl.operators.cloning;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Clone;
import org.gradoop.flink.model.impl.functions.epgm.ElementIdUpdater;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.PairElementWithNewId;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.epgm.EdgeSourceUpdateJoin;
import org.gradoop.flink.model.impl.functions.epgm.EdgeTargetUpdateJoin;
import org.gradoop.flink.model.impl.operators.cloning.functions.ElementGraphUpdater;
import org.gradoop.flink.model.impl.operators.cloning.functions.Value0Of2ToId;

/**
 * Creates a copy of the graph instance with new ids for the graph head, vertices and edges.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public class Cloning<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements UnaryBaseGraphToBaseGraphOperator<LG> {

  @Override
  public LG execute(LG graph) {

    //--------------------------------------------------------------------------
    // compute new graphs
    //--------------------------------------------------------------------------

    DataSet<G> graphHead = graph.getGraphHead()
      .map(new Clone<>());

    DataSet<GradoopId> graphId = graphHead.map(new Id<>());

    //--------------------------------------------------------------------------
    // compute new vertices
    //--------------------------------------------------------------------------

    DataSet<Tuple2<V, GradoopId>> vertexTuple = graph.getVertices()
        .map(new PairElementWithNewId<>());

    DataSet<Tuple2<GradoopId, GradoopId>> vertexIdTuple = vertexTuple
      .map(new Value0Of2ToId<>());

    DataSet<V> vertices = vertexTuple
      .map(new ElementIdUpdater<>())
      //update graph ids
      .map(new ElementGraphUpdater<>())
      .withBroadcastSet(graphId, ElementGraphUpdater.GRAPHID);

    //--------------------------------------------------------------------------
    // compute new edges
    //--------------------------------------------------------------------------

    DataSet<E> edges = graph.getEdges()
      .map(new Clone<>())
      //update source vertex ids
      .join(vertexIdTuple)
      .where(new SourceId<>()).equalTo(0)
      .with(new EdgeSourceUpdateJoin<>())
      //update target vertex ids
      .join(vertexIdTuple)
      .where(new TargetId<>()).equalTo(0)
      .with(new EdgeTargetUpdateJoin<>())
      //update graph ids
      .map(new ElementGraphUpdater<>())
      .withBroadcastSet(graphId, ElementGraphUpdater.GRAPHID);

    return graph.getFactory().fromDataSets(graphHead, vertices, edges);
  }
}
