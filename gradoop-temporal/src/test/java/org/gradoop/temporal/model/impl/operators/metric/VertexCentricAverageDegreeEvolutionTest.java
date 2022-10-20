/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.metric;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link VertexCentricAverageDegreeEvolution}.
 */
@RunWith(Parameterized.class)
public class VertexCentricAverageDegreeEvolutionTest extends TemporalGradoopTestBase {
  /**
   * The label of the vertex to test.
   */
  @Parameterized.Parameter(0)
  public String vertexLabel;
  /**
   * The degree type to test.
   */
  @Parameterized.Parameter(1)
  public VertexDegree degreeType;
  /**
   * The user-defined start of the interval.
   */
  @Parameterized.Parameter(2)
  public Long queryStart;
  /**
   * The user-defined end of the interval.
   */
  @Parameterized.Parameter(3)
  public Long queryEnd;
  /**
   * The expected average degree.
   */
  @Parameterized.Parameter(4)
  public Double expectedAvgDegree;
  /**
   * The temporal graph to test the operator.
   */
  TemporalGraph testGraph;
  /**
   * The vertex-id to test.
   */
  GradoopId vertexId;

  /**
   * The parameters to test the operator with the test inputs and the expected results.
   */
  @Parameterized.Parameters(name = "Test vertex label {0} with degree type {1}, start: {2}, end: {3}")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(
          new Object[] {"v1", VertexDegree.OUT, -3L, -1L, 0.0},
          new Object[] {"v1", VertexDegree.OUT, -2L, 5L, 5.0 / 7.0},
          new Object[] {"v2", VertexDegree.BOTH, 0L, 10L, 1.1},
          new Object[] {"v3", VertexDegree.IN, 3L, 6L, 0.0},
          new Object[] {"v4", VertexDegree.BOTH, 4L, 5L, 3.0},
          new Object[] {"v5", VertexDegree.IN, 2L, 5L, 2.0 / 3.0}
    );
  }

  /**
   * Set up the test graph and find the corresponding vertex-id for the label to be tested.
   *
   * @throws Exception in case of an error
   */
  @Before
  public void setUp() throws Exception {
    testGraph = getTestGraphWithValues();
    vertexId = testGraph.getVerticesByLabel(vertexLabel).collect().get(0).getId();
  }

  /**
   * Test the vertex-centric maximum degree operator.
   *
   * @throws Exception in case of an error
   */
  @Test
  public void testVertexCentricAverageDegree() throws Exception {
    VertexCentricAverageDegreeEvolution operator = new VertexCentricAverageDegreeEvolution(degreeType,
      TimeDimension.VALID_TIME, vertexId, queryStart, queryEnd);
    Double result = testGraph.callForValue(operator).collect().get(0).f0;
    assertEquals(expectedAvgDegree, result);
  }
}
