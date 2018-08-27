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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.vertexdegrees.DistinctVertexDegrees;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;

public class RandomLimitedDegreeVertexSamplingTest extends ParametrizedTestForGraphSampling {

  /**
   * Creates a new RandomLimitedDegreeVertexSamplingTest instance.
   *
   * @param testName Name for test-case
   * @param seed Seed-value for random number generator, e.g. 0
   * @param sampleSize Value for sample size, e.g. 0.5
   * @param degreeType The vertex degree type, e.g. VertexDegree.BOTH
   * @param degreeThreshold The threshold for the vertex degree, e.g. 3
   */
  public RandomLimitedDegreeVertexSamplingTest(String testName, String seed, String sampleSize,
    String degreeType, String degreeThreshold) {
    super(testName, Long.parseLong(seed), Float.parseFloat(sampleSize),
      VertexDegree.valueOf(degreeType), Long.parseLong(degreeThreshold));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SamplingAlgorithm getSamplingOperator() {
    return new RandomLimitedDegreeVertexSampling(sampleSize, seed, degreeThreshold, degreeType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void validateSpecific(LogicalGraph input, LogicalGraph output) {
    List<Vertex> dbDegreeVertices = Lists.newArrayList();
    LogicalGraph inputWithDegrees = new DistinctVertexDegrees(
      VertexDegree.BOTH.getName(),
      VertexDegree.IN.getName(),
      VertexDegree.OUT.getName(),
      true).execute(input);
    inputWithDegrees.getVertices().output(new LocalCollectionOutputFormat<>(dbDegreeVertices));

    try {
      getExecutionEnvironment().execute();
    } catch (Exception e) {
      e.printStackTrace();
    }

    dbEdges.removeAll(newEdges);
    for (Edge edge : dbEdges) {
      assertFalse("edge from original graph was not sampled but source and target were",
        newVertexIDs.contains(edge.getSourceId()) &&
          newVertexIDs.contains(edge.getTargetId()));
    }

    List<Vertex> verticesSampledByDegree = Lists.newArrayList();
    for (Vertex vertex : dbDegreeVertices) {
      if ((Long.parseLong(vertex.getPropertyValue(degreeType.getName()).toString()) > degreeThreshold) &&
        newVertices.contains(vertex)) {
        verticesSampledByDegree.add(vertex);
      }
    }
    dbDegreeVertices.removeAll(verticesSampledByDegree);
    for (Vertex vertex : dbDegreeVertices) {
      assertFalse("vertex with degree greater than degree threshold was not sampled",
        Long.parseLong(vertex.getPropertyValue(degreeType.getName()).toString()) > degreeThreshold);
    }
  }

  /**
   * Parameters called when running the test
   *
   * @return List of parameters
   */
  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(
      new String[] {
        "With seed and the sum of in- and out-degree with value = 3",
        "-4181668494294894490",
        "0.272f",
        "BOTH",
        "3"
      },
      new String[] {
        "Without seed and the sum of in- and out-degree with value = 3",
        "0",
        "0.272f",
        "BOTH",
        "3"
      },
      new String[] {
        "With seed and vertex input degree for value = 3",
        "-4181668494294894490",
        "0.272f",
        "IN",
        "3"
      },
      new String[] {
        "With seed and vertex input degree for value = 0",
        "-4181668494294894490",
        "0.272f",
        "IN",
        "0"
      },
      new String[] {
        "With seed and vertex input degree for value = -1",
        "-4181668494294894490",
        "0.272f",
        "IN",
        "-1"
      },
      new String[] {
        "With seed and vertex output degree for value = 3",
        "-4181668494294894490",
        "0.272f",
        "OUT",
        "3"
      },
      new String[] {
        "With seed and vertex output degree for value = 0",
        "-4181668494294894490",
        "0.272f",
        "OUT",
        "0"
      },
      new String[] {
        "With seed and vertex output degree for value = -1",
        "-4181668494294894490",
        "0.272f",
        "OUT",
        "-1"
      });
  }
}