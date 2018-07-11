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

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.vertexdegrees.DistinctVertexDegrees;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class RandomLimitedDegreeVertexSamplingTest extends GradoopFlinkTestBase {

  private final String testName;

  private final long randomSeed;

  private final float sampleSize;

  private final VertexDegree.DegreeType degreeType;

  private final long degreeThreshold;

  private final String degreePropertyKey;

  public RandomLimitedDegreeVertexSamplingTest(String testName, String randomSeed,
    String sampleSize, String degreeType, String degreeThreshold) {
    this.testName = testName;
    this.randomSeed = Long.parseLong(randomSeed);
    this.sampleSize = Float.parseFloat(sampleSize);
    this.degreeType = VertexDegree.fromString(degreeType);
    this.degreeThreshold = Long.parseLong(degreeThreshold);

    if(this.degreeType == VertexDegree.DegreeType.InputDegree) {
      this.degreePropertyKey = VertexDegree.IN_DEGREE_PROPERTY_NAME;
    } else if(this.degreeType == VertexDegree.DegreeType.OutputDegree) {
      this.degreePropertyKey = VertexDegree.OUT_DEGREE_PROPERTY_NAME;
    } else if(this.degreeType == VertexDegree.DegreeType.Degree) {
      this.degreePropertyKey = VertexDegree.DEGREE_PROPERTY_NAME;
    } else {
      throw new InvalidParameterException();
    }
  }

  @Test
  public void randomLimitedDegreeVertexSamplingTest() throws Exception {
    LogicalGraph dbGraph = getSocialNetworkLoader()
      .getDatabase().getDatabaseGraph();

    LogicalGraph newGraph = new RandomLimitedDegreeVertexSampling(sampleSize, randomSeed,
      degreeThreshold, degreeType).execute(dbGraph);

    validateResult(dbGraph, newGraph);
  }

  private void validateResult(LogicalGraph input, LogicalGraph output)
    throws Exception {
    List<Vertex> dbVertices = Lists.newArrayList();
    List<Edge> dbEdges = Lists.newArrayList();
    List<Vertex> newVertices = Lists.newArrayList();
    List<Edge> newEdges = Lists.newArrayList();

    LogicalGraph inputWithDegrees = new DistinctVertexDegrees(VertexDegree.DEGREE_PROPERTY_NAME,
      VertexDegree.IN_DEGREE_PROPERTY_NAME,VertexDegree.OUT_DEGREE_PROPERTY_NAME,
      true).execute(input);

    inputWithDegrees.getVertices().output(new LocalCollectionOutputFormat<>(dbVertices));
    inputWithDegrees.getEdges().output(new LocalCollectionOutputFormat<>(dbEdges));

    output.getVertices().output(new LocalCollectionOutputFormat<>(newVertices));
    output.getEdges().output(new LocalCollectionOutputFormat<>(newEdges));

    getExecutionEnvironment().execute();

    // Test, if there is a result graph
    assertNotNull("graph was null", output);

    Set<GradoopId> newVertexIDs = new HashSet<>();
    for (Vertex vertex : newVertices) {
      // Test, if all new vertices are taken from the original graph
      assertTrue("sampled vertex is not part of the original graph",
        dbVertices.contains(vertex));
      newVertexIDs.add(vertex.getId());
    }

    for (Edge edge : newEdges) {
      // Test, if all new edges are taken from the original graph
      assertTrue("sampled edge is not part of the original graph", dbEdges.contains(edge));
      // Test, if all source- and target-vertices from new edges are part of the sampled graph, too
      assertTrue("sampled edge has source vertex which is not part of the sampled graph",
        newVertexIDs.contains(edge.getSourceId()));
      assertTrue("sampled edge has target vertex which is not part of the sampled graph",
        newVertexIDs.contains(edge.getTargetId()));
    }

    // Test, if there aren't any source- and target-vertices from not sampled edges left
    dbEdges.removeAll(newEdges);
    for (Edge edge : dbEdges) {
      assertFalse("there are vertices from edges, which are not part of the sampled graph",
        newVertexIDs.contains(edge.getSourceId()) &&
          newVertexIDs.contains(edge.getTargetId()));
    }

    // Test, if all vertices with degree higher the given threshold were sampled
    List<Vertex> verticesSampledByDegree = Lists.newArrayList();
    for (Vertex vertex : dbVertices) {
      if ((Long.parseLong(vertex.getPropertyValue(degreePropertyKey).toString())
        > degreeThreshold) && newVertices.contains(vertex)) {
        verticesSampledByDegree.add(vertex);
      }
    }
    dbVertices.removeAll(verticesSampledByDegree);
    for (Vertex vertex : dbVertices) {
      assertFalse("vertex with degree higher than degree threshold was not sampled",
        Long.parseLong(vertex.getPropertyValue(degreePropertyKey).toString())
          > degreeThreshold);
    }
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(
      new String[] {
        "With seed and the sum of both vertex degrees",
        "-4181668494294894490",
        "0.272f",
        "Degree",
        "3"
      },
      new String[] {
        "Without seed and the sum of both vertex degrees",
        "0",
        "0.272f",
        "Degree",
        "3"
      },
      new String[] {
        "With seed and vertex input degree",
        "-4181668494294894490",
        "0.272f",
        "InputDegree",
        "3"
      },
      new String[] {
        "With seed and vertex output degree",
        "-4181668494294894490",
        "0.272f",
        "OutputDegree",
        "3"
      });
  }
}