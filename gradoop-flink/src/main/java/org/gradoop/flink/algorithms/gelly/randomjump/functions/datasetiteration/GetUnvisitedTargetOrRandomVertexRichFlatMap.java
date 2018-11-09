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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.List;
import java.util.Random;

/**
 * <pre>
 * Maps the currently active tuple containing a source vertex to a vertex id, which is either:
 *  - a randomly chosen target from all not yet visited edges from this source vertex (walk), or
 *  - a randomly chosen vertex from the graph (jump),
 * depending on the probability to jump or if there are any unvisited edges left for a walk.
 * </pre>
 */
public class GetUnvisitedTargetOrRandomVertexRichFlatMap extends
  RichFlatMapFunction<IterativeTuple, GradoopId> {

  /**
   * Name of the broadcast set containing the graphs vertex ids
   */
  private final String vertexIdsBroadcastSet;

  /**
   * Set containing the graphs vertex ids
   */
  private List<GradoopId> vertexIds;

  /**
   * Random generator
   */
  private final Random random;

  /**
   * Creates an instance of GetUnvisitedTargetOrRandomVertexRichFlatMap.
   *
   * @param vertexIdsBroadcastSet Name of the broadcast set containing all vertex ids
   */
  public GetUnvisitedTargetOrRandomVertexRichFlatMap(String vertexIdsBroadcastSet) {
    this.vertexIdsBroadcastSet = vertexIdsBroadcastSet;
    this.random = new Random();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    vertexIds = getRuntimeContext().getBroadcastVariable(vertexIdsBroadcastSet);
  }

  @Override
  public void flatMap(IterativeTuple iterativeTuple, Collector<GradoopId> out) throws Exception {
    List<IterativeTuple.EdgeType> unvisitedEdges = iterativeTuple.getUnvisitedEdges();
    GradoopId newVertexId;
    if (!unvisitedEdges.isEmpty() &&
      (unvisitedEdges.get(0).getJumpProbability() < random.nextDouble())) {
      int randomIndex = random.nextInt(unvisitedEdges.size());
      newVertexId = unvisitedEdges.get(randomIndex).getTargetId();
    } else {
      int randomIndex = random.nextInt(vertexIds.size());
      newVertexId = vertexIds.get(randomIndex);
    }
    out.collect(newVertexId);
  }
}
