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
package org.gradoop.flink.model.impl.operators.sampling;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.functions.Neighborhood;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Parameterized test-class from which all sampling test-classes are derived.
 * Derived test-classes need to define the parameters as {@code Iterable data()}
 */
@RunWith(Parameterized.class)
public abstract class ParametrizedTestForGraphSampling extends GradoopFlinkTestBase {
  /**
   * Name for test-case
   */
  private String testName;
  /**
   * Seed-value for random number generator, e.g. 0L
   */
  long seed;
  /**
   * Value for sample size, e.g. 0.5
   */
  float sampleSize;
  /**
   * Value for edge sample size (if sampled separately), e.g. 0.5
   */
  float edgeSampleSize;
  /**
   * The vertex neighborhood type. Distinguishes ingoing, outgoing edges or both.
   */
  Neighborhood neighborType = Neighborhood.BOTH;
  /**
   * The dampening factor used by Flinks PageRank-algorithm
   */
  double dampeningFactor = 0.5;
  /**
   * The iteration number used by Flinks PageRank-algorithm
   */
  int maxIteration = 20;
  /**
   * Whether to sample vertices with PageRank-score greater (true) or equal/smaller (false) the
   * sampleSize
   */
  boolean sampleGreaterThanThreshold = true;
  /**
   * Whether to sample all vertices (true) or none of them (false), in case all vertices got the
   * same PageRank-score.
   */
  boolean keepVerticesIfSameScore;
  /**
   * The vertex degree type. Distinguishes in-degree, out-degree or the sum of both.
   */
  VertexDegree degreeType = VertexDegree.BOTH;
  /**
   * The threshold for the vertex degree
   */
  long degreeThreshold = 3L;
  /**
   * Sampling type for VertexEdgeSampling
   */
  RandomVertexEdgeSampling.VertexEdgeSamplingType vertexEdgeSamplingType =
    RandomVertexEdgeSampling.VertexEdgeSamplingType.SimpleVersion;

  /**
   * List of original vertices
   */
  List<Vertex> dbVertices;
  /**
   * List of original edges
   */
  List<Edge> dbEdges;
  /**
   * List of sampled vertices
   */
  List<Vertex> newVertices;
  /**
   * IDs from sampled vertices
   */
  Set<GradoopId> newVertexIDs;
  /**
   * List of sampled edges
   */
  List<Edge> newEdges;

  /**
   * Common Constructor for most samplings
   *
   * @param testName Name for test-case
   * @param seed Seed-value for random number generator, e.g. 0
   * @param sampleSize Value for sample size, e.g. 0.5
   */
  public ParametrizedTestForGraphSampling(String testName, long seed, float sampleSize) {
    this.testName = testName;
    this.seed = seed;
    this.sampleSize = sampleSize;
  }

  /**
   * Constructor for VertexNeighborhoodSamplingTest
   *
   * @param testName Name for test-case
   * @param seed Seed-value for random number generator, e.g. 0
   * @param sampleSize Value for sample size, e.g. 0.5
   * @param neighborType The vertex neighborhood type, e.g. Neighborhood.BOTH
   */
  public ParametrizedTestForGraphSampling(String testName, long seed, float sampleSize,
    Neighborhood neighborType) {
    this(testName, seed, sampleSize);
    this.neighborType = neighborType;
  }

  /**
   * Constructor for PageRankSamplingTest
   *
   * @param testName Name for test-case
   * @param seed Seed-value for random number generator, e.g. 0
   * @param sampleSize Value for sample size, e.g. 0.5
   * @param dampeningFactor The dampening factor used by Flinks PageRank-algorithm, e.g. 0.85
   * @param maxIteration The iteration number used by Flinks PageRank-algorithm, e.g. 20
   */
  public ParametrizedTestForGraphSampling(String testName, long seed, float sampleSize,
    double dampeningFactor, int maxIteration, boolean sampleGreaterThanThreshold,
    boolean keepVerticesIfSameScore) {
    this(testName, seed, sampleSize);
    this.dampeningFactor = dampeningFactor;
    this.maxIteration = maxIteration;
    this.sampleGreaterThanThreshold = sampleGreaterThanThreshold;
    this.keepVerticesIfSameScore = keepVerticesIfSameScore;
  }

  /**
   * Constructor for LimitedDegreeVertexSamplingTest
   *
   * @param testName Name for test-case
   * @param seed Seed-value for random number generator, e.g. 0
   * @param sampleSize Value for sample size, e.g. 0.5
   * @param degreeType The vertex degree type, e.g. VertexDegree.BOTH
   * @param degreeThreshold The threshold for the vertex degree, e.g. 3
   */
  public ParametrizedTestForGraphSampling(String testName, long seed, float sampleSize,
    VertexDegree degreeType, long degreeThreshold) {
    this(testName, seed, sampleSize);
    this.degreeType = degreeType;
    this.degreeThreshold = degreeThreshold;
  }

  /**
   * Constructor for VertexEdgeSamplingTest
   *
   * @param testName Name for test-case
   * @param seed Seed-value for random number generator, e.g. 0
   * @param sampleSize Value for vertex sample size, e.g. 0.5
   * @param edgeSampleSize Value for edge sample size, e.g. 0.5
   * @param vertexEdgeSamplingType Type for VertexEdgeSampling, e.g. SimpleVersion
   */
  public ParametrizedTestForGraphSampling(String testName, long seed, float sampleSize,
    float edgeSampleSize, RandomVertexEdgeSampling.VertexEdgeSamplingType vertexEdgeSamplingType) {
    this(testName, seed, sampleSize);
    this.edgeSampleSize = edgeSampleSize;
    this.vertexEdgeSamplingType = vertexEdgeSamplingType;
  }

  /**
   * Gets the sampling operator used for testing.
   *
   * @return The sampling operator
   */
  public abstract SamplingAlgorithm getSamplingOperator();

  /**
   * Common test-case.
   *
   * @throws Exception Exception thrown by graph loading
   */
  @Test
  public void samplingTest() throws Exception {
    LogicalGraph dbGraph = getSocialNetworkLoader().getLogicalGraph();
    LogicalGraph newGraph = getSamplingOperator().sample(dbGraph);

    validateGraph(dbGraph, newGraph);
    validateSpecific(dbGraph, newGraph);
  }

  /**
   * The common graph validation used by all graph samplings
   *
   * @param input The input graph
   * @param output The sampled graph
   */
  private void validateGraph(LogicalGraph input, LogicalGraph output) throws Exception {
    dbVertices = Lists.newArrayList();
    dbEdges = Lists.newArrayList();
    newVertices = Lists.newArrayList();
    newEdges = Lists.newArrayList();

    input.getVertices().output(new LocalCollectionOutputFormat<>(dbVertices));
    input.getEdges().output(new LocalCollectionOutputFormat<>(dbEdges));

    output.getVertices().output(new LocalCollectionOutputFormat<>(newVertices));
    output.getEdges().output(new LocalCollectionOutputFormat<>(newEdges));

    getExecutionEnvironment().execute();

    assertNotNull("graph was null", output);

    newVertexIDs = new HashSet<>();
    for (Vertex vertex : newVertices) {
      assertTrue("sampled vertex is not part of the original graph", dbVertices.contains(vertex));
      newVertexIDs.add(vertex.getId());
    }
    for (Edge edge : newEdges) {
      assertTrue("sampled edge is not part of the original graph", dbEdges.contains(edge));
      assertTrue("sampled edge has source vertex which is not part of the sampled graph",
        newVertexIDs.contains(edge.getSourceId()));
      assertTrue("sampled edge has target vertex which is not part of the sampled graph",
        newVertexIDs.contains(edge.getTargetId()));
    }
  }

  /**
   * The specific graph validation used by some graph samplings.
   * Needs to be implemented in the respective sampling test-class.
   *
   * @param input The input graph
   * @param output The sampled graph
   */
  public abstract void validateSpecific(LogicalGraph input, LogicalGraph output);
}
