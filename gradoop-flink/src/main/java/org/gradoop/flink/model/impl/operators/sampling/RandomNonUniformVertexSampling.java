/**
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
package org.gradoop.flink.model.impl.operators.sampling;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.vertexdegrees.DistinctVertexDegrees;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.sampling.functions.AddMaxDegreeCrossFunction;
import org.gradoop.flink.model.impl.operators.sampling.functions.NonUniformVertexRandomFilter;
import org.gradoop.flink.model.impl.operators.sampling.functions.RemoveUnnecessaryPropertiesMap;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexToDegreeMap;

import java.util.Vector;

/**
 * Computes a vertex sampling of the graph. Retains randomly chosen vertices of a given relative
 * amount and all edges which source- and target-vertices where chosen. A degree-dependent value
 * is taken into account to have a bias towards high-degree vertices. There may retain some
 * unconnected vertices in the sampled graph.
 */
public class RandomNonUniformVertexSampling implements UnaryGraphToGraphOperator {
  /**
   * relative amount of vertices in the result graph
   */
  private final float sampleSize;

  /**
   * seed for the random number generator
   * if seed is null, the random generator is created without seed
   */
  private final long randomSeed;

  /**
   * Creates new RandomNonUniformVertexSampling instance.
   *
   * @param sampleSize relative sample size
   */
  public RandomNonUniformVertexSampling(float sampleSize) {
    this(sampleSize, 0L);
  }

  /**
   * Creates new RandomNonUniformVertexSampling instance.
   *
   * @param sampleSize relative sample size
   * @param randomSeed random seed value (can be {@code null})
   */
  public RandomNonUniformVertexSampling(float sampleSize, long randomSeed) {
    this.sampleSize = sampleSize;
    this.randomSeed = randomSeed;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {

    graph = new DistinctVertexDegrees("deg", "indeg", "outdeg",
    true).execute(graph);

    DataSet<Vertex> newVertices = graph.getVertices()
      .map(new VertexToDegreeMap("deg"))
      .max(0).cross(graph.getVertices())
      .with(new AddMaxDegreeCrossFunction("maxdeg"));

    graph = graph.getConfig().getLogicalGraphFactory().fromDataSets(newVertices, graph.getEdges());

    newVertices = graph.getVertices().filter(
      new NonUniformVertexRandomFilter<>(sampleSize, randomSeed, "deg","maxdeg"));
    Vector<String> unnecessaryPropertyNames = new Vector<>();
    unnecessaryPropertyNames.add("deg");
    unnecessaryPropertyNames.add("indeg");
    unnecessaryPropertyNames.add("outdeg");
    unnecessaryPropertyNames.add("maxdeg");
    newVertices = newVertices.map(new RemoveUnnecessaryPropertiesMap<>(unnecessaryPropertyNames));

    DataSet<Edge> newEdges = graph.getEdges()
      .join(newVertices)
      .where(new SourceId<>())
      .equalTo(new Id<>())
      .with(new LeftSide<>())
      .join(newVertices)
      .where(new TargetId<>())
      .equalTo(new Id<>())
      .with(new LeftSide<>());

    return graph.getConfig().getLogicalGraphFactory().fromDataSets(newVertices, newEdges);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() { return RandomNonUniformVertexSampling.class.getName(); }
}
