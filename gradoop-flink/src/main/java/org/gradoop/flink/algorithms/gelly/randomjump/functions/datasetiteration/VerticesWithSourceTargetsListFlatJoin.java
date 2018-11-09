/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.IterativeTuple.EdgeType;

import java.util.ArrayList;
import java.util.List;

/**
 * Joins the graph vertices with a set of {@code Tuple2<GradoopId, List<EdgeType>>}, representing
 * a source vertex id and a list of its outgoing edges of type {@link EdgeType}. Returns a tuple of
 * type {@link IterativeTuple} used for the iterative dataset. Assigns the edge list to this tuple
 * in case of a match, assigns an empty list otherwise.
 */
public class VerticesWithSourceTargetsListFlatJoin implements
  FlatJoinFunction<Vertex, Tuple2<GradoopId, List<EdgeType>>, IterativeTuple> {

  @Override
  public void join(Vertex vertex, Tuple2<GradoopId, List<EdgeType>> sourceEdgesListTuple,
    Collector<IterativeTuple> out) throws Exception {
    IterativeTuple iterativeTuple = new IterativeTuple(
      0L, vertex.getId(), 0L, new ArrayList<>());
    if (sourceEdgesListTuple != null) {
      iterativeTuple.setEdges(sourceEdgesListTuple.f1);
    }
    out.collect(iterativeTuple);
  }
}
