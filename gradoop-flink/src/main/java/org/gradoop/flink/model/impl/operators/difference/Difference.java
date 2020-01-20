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
package org.gradoop.flink.model.impl.operators.difference;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.operators.base.SetOperatorBase;
import org.gradoop.flink.model.impl.operators.difference.functions.CreateTuple2WithLong;
import org.gradoop.flink.model.impl.operators.difference.functions.IdOf0InTuple2;
import org.gradoop.flink.model.impl.operators.difference.functions.RemoveCut;

/**
 * Returns a collection with all base graphs that are contained in the
 * first input collection but not in the second.
 * Graph equality is based on their respective identifiers.
 *
 * @see DifferenceBroadcast
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the base graph instance
 * @param <GC> type of the graph collection
 */
public class Difference<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends SetOperatorBase<G, V, E, LG, GC> {

  @Override
  protected DataSet<G> computeNewGraphHeads() {
    // assign 1L to each graph in the first collection
    DataSet<Tuple2<G, Long>> thisGraphs = firstCollection
      .getGraphHeads()
      .map(new CreateTuple2WithLong<>(1L));
    // assign 2L to each graph in the second collection
    DataSet<Tuple2<G, Long>> otherGraphs = secondCollection
      .getGraphHeads()
      .map(new CreateTuple2WithLong<>(2L));

    // union the graphs, group them by their identifier and check that
    // there is no graph in the group that belongs to the second collection
    return thisGraphs
      .union(otherGraphs)
      .groupBy(new IdOf0InTuple2<>())
      .reduceGroup(new RemoveCut<>());
  }
}
