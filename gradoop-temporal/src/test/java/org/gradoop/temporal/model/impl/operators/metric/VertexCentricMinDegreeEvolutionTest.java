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

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class VertexCentricMinDegreeEvolutionTest extends TemporalGradoopTestBase {

  /**
   * The expected minimum degrees for this test.
   */
  private static final List<Integer> EXPECTED_MIN_DEGREES = new ArrayList<>();

  static {
    EXPECTED_MIN_DEGREES.add(0);
    EXPECTED_MIN_DEGREES.add(0);
    EXPECTED_MIN_DEGREES.add(1);
    EXPECTED_MIN_DEGREES.add(0);
    EXPECTED_MIN_DEGREES.add(3);
    EXPECTED_MIN_DEGREES.add(0);
  }

  /**
   * The degree type to test.
   */
  public VertexDegree degreeType;

  /**
   * The temporal graph to test the operator.
   */
  TemporalGraph testGraph;

  /**
   * A list of all vertex-ids.
   */
  List<GradoopId> vertexIds;

  /**
   * The vertex-id to test.
   */
  GradoopId vertexId;

  /**
   * The user-defined start of the interval.
   */
  Long queryStart;

  /**
   * The user-defined end of the interval.
   */
  Long queryEnd;

  /**
   * Set up the test graph and extract all vertex-ids.
   *
   * @throws Exception in case of an error
   */
  @Before
  public void setUp() throws Exception {
    testGraph = getTestGraphWithValues();
    vertexIds = testGraph.getVertices().map(v -> v.getId()).collect();
  }

  @Test
  public void testVertexCentricMinDegree() throws Exception {
    setUp();

    // Test 1
    vertexId = vertexIds.get(0);
    degreeType = VertexDegree.OUT;
    queryStart = -3L;
    queryEnd = -1L;
    VertexCentricMinDegreeEvolution operator = new VertexCentricMinDegreeEvolution(degreeType, TimeDimension.VALID_TIME, vertexId, queryStart, queryEnd);
    Integer result = testGraph.callForValue(operator).collect().get(0).f0;
    assertEquals(EXPECTED_MIN_DEGREES.get(0), result);

    // Test 2
    queryStart = -2L;
    queryEnd = 5L;
    operator = new VertexCentricMinDegreeEvolution(degreeType, TimeDimension.VALID_TIME, vertexId, queryStart, queryEnd);
    result = testGraph.callForValue(operator).collect().get(0).f0;
    assertEquals(EXPECTED_MIN_DEGREES.get(1), result);

    // Test 3
    vertexId = vertexIds.get(1);
    degreeType = VertexDegree.BOTH;
    queryStart = 0L;
    queryEnd = 10L;
    operator = new VertexCentricMinDegreeEvolution(degreeType, TimeDimension.VALID_TIME, vertexId, queryStart, queryEnd);
    result = testGraph.callForValue(operator).collect().get(0).f0;
    assertEquals(EXPECTED_MIN_DEGREES.get(2), result);

    // Test 4
    vertexId = vertexIds.get(2);
    degreeType = VertexDegree.IN;
    queryStart = 3L;
    queryEnd = 6L;
    operator = new VertexCentricMinDegreeEvolution(degreeType, TimeDimension.VALID_TIME, vertexId, queryStart, queryEnd);
    result = testGraph.callForValue(operator).collect().get(0).f0;
    assertEquals(EXPECTED_MIN_DEGREES.get(3), result);

    // Test 5
    vertexId = vertexIds.get(3);
    degreeType = VertexDegree.BOTH;
    queryStart = 4L;
    queryEnd = 5L;
    operator = new VertexCentricMinDegreeEvolution(degreeType, TimeDimension.VALID_TIME, vertexId, queryStart, queryEnd);
    result = testGraph.callForValue(operator).collect().get(0).f0;
    assertEquals(EXPECTED_MIN_DEGREES.get(4), result);

    // Test 6
    vertexId = vertexIds.get(4);
    degreeType = VertexDegree.IN;
    queryStart = 2L;
    queryEnd = 5L;
    operator = new VertexCentricMinDegreeEvolution(degreeType, TimeDimension.VALID_TIME, vertexId, queryStart, queryEnd);
    result = testGraph.callForValue(operator).collect().get(0).f0;
    assertEquals(EXPECTED_MIN_DEGREES.get(5), result);
  }
}
