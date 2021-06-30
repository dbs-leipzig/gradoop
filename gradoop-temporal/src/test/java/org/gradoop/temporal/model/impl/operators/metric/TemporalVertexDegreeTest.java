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

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link TemporalVertexDegree}.
 */
@RunWith(Parameterized.class)
public class TemporalVertexDegreeTest extends TemporalGradoopTestBase {
  /**
   * The expected in-degrees for each vertex label.
   */
  private static final HashMap<String, List<Tuple3<Long, Long, Integer>>> EXPECTED_IN_DEGREES =
    new HashMap<>();
  /**
   * The expected out-degrees for each vertex label.
   */
  private static final HashMap<String, List<Tuple3<Long, Long, Integer>>> EXPECTED_OUT_DEGREES =
    new HashMap<>();
  /**
   * The expected degrees for each vertex label.
   */
  private static final HashMap<String, List<Tuple3<Long, Long, Integer>>> EXPECTED_BOTH_DEGREES =
    new HashMap<>();
  /**
   * The expected in-degrees for each vertex label.
   */
  private static final HashMap<String, List<Tuple3<Long, Long, Integer>>> EXPECTED_IN_DEGREES_DEF =
    new HashMap<>();
  /**
   * The expected out-degrees for each vertex label.
   */
  private static final HashMap<String, List<Tuple3<Long, Long, Integer>>> EXPECTED_OUT_DEGREES_DEF =
    new HashMap<>();
  /**
   * The expected degrees for each vertex label.
   */
  private static final HashMap<String, List<Tuple3<Long, Long, Integer>>> EXPECTED_BOTH_DEGREES_DEF =
    new HashMap<>();

  static {
    // IN DEGREES with vertex time included
    EXPECTED_IN_DEGREES.put(V1,
      Collections.singletonList(new Tuple3<>(Long.MIN_VALUE, Long.MAX_VALUE, 0)));
    EXPECTED_IN_DEGREES.put(V2,
      Collections.singletonList(new Tuple3<>(0L, Long.MAX_VALUE, 1)));
    EXPECTED_IN_DEGREES.put(V3,
      Arrays.asList(new Tuple3<>(3L, 6L, 0), new Tuple3<>(6L, 7L, 1), new Tuple3<>(7L, 9L, 0)));
    EXPECTED_IN_DEGREES.put(V4,
      Collections.singletonList(new Tuple3<>(4L, 5L, 1)));
    EXPECTED_IN_DEGREES.put(V5,
      Arrays.asList(new Tuple3<>(1L, 4L, 0), new Tuple3<>(4L, 5L, 2), new Tuple3<>(5L, 6L, 1),
        new Tuple3<>(6L, 9L, 0)));

    // IN DEGREES without vertex time included
    EXPECTED_IN_DEGREES_DEF.put(V1,
      Collections.singletonList(new Tuple3<>(Long.MIN_VALUE, Long.MAX_VALUE, 0)));
    EXPECTED_IN_DEGREES_DEF.put(V2,
      Arrays.asList(new Tuple3<>(Long.MIN_VALUE, 0L, 0), new Tuple3<>(0L, Long.MAX_VALUE, 1)));
    EXPECTED_IN_DEGREES_DEF.put(V3,
      Arrays.asList(new Tuple3<>(Long.MIN_VALUE, 6L, 0), new Tuple3<>(6L, 7L, 1),
        new Tuple3<>(7L, Long.MAX_VALUE, 0)));
    EXPECTED_IN_DEGREES_DEF.put(V4,
      Arrays.asList(new Tuple3<>(Long.MIN_VALUE, 4L, 0), new Tuple3<>(4L, 5L, 1),
        new Tuple3<>(5L, Long.MAX_VALUE, 0)));
    EXPECTED_IN_DEGREES_DEF.put(V5,
      Arrays.asList(new Tuple3<>(Long.MIN_VALUE, 4L, 0), new Tuple3<>(4L, 5L, 2), new Tuple3<>(5L, 6L, 1),
        new Tuple3<>(6L, Long.MAX_VALUE, 0)));

    // OUT DEGREES with vertex time included
    EXPECTED_OUT_DEGREES.put(V1,
      Arrays.asList(new Tuple3<>(Long.MIN_VALUE, 0L, 0), new Tuple3<>(0L, Long.MAX_VALUE, 1)));
    EXPECTED_OUT_DEGREES.put(V2,
      Arrays.asList(new Tuple3<>(0L, 6L, 0), new Tuple3<>(6L, 7L, 1), new Tuple3<>(7L, Long.MAX_VALUE, 0)));
    EXPECTED_OUT_DEGREES.put(V3,
      Arrays.asList(new Tuple3<>(3L, 4L, 0), new Tuple3<>(4L, 6L, 1), new Tuple3<>(6L, 9L, 0)));
    EXPECTED_OUT_DEGREES.put(V4,
      Collections.singletonList(new Tuple3<>(4L, 5L, 2)));
    EXPECTED_OUT_DEGREES.put(V5,
      Collections.singletonList(new Tuple3<>(1L, 9L, 0)));

    // OUT DEGREES without vertex time included
    EXPECTED_OUT_DEGREES_DEF.put(V1,
      Arrays.asList(new Tuple3<>(Long.MIN_VALUE, 0L, 0), new Tuple3<>(0L, Long.MAX_VALUE, 1)));
    EXPECTED_OUT_DEGREES_DEF.put(V2,
      Arrays.asList(new Tuple3<>(Long.MIN_VALUE, 6L, 0), new Tuple3<>(6L, 7L, 1),
        new Tuple3<>(7L, Long.MAX_VALUE, 0)));
    EXPECTED_OUT_DEGREES_DEF.put(V3,
      Arrays.asList(new Tuple3<>(Long.MIN_VALUE, 4L, 0), new Tuple3<>(4L, 6L, 1),
        new Tuple3<>(6L, Long.MAX_VALUE, 0)));
    EXPECTED_OUT_DEGREES_DEF.put(V4,
      Arrays.asList(new Tuple3<>(Long.MIN_VALUE, 4L, 0), new Tuple3<>(4L, 5L, 2),
        new Tuple3<>(5L, Long.MAX_VALUE, 0)));
    EXPECTED_OUT_DEGREES_DEF.put(V5,
      Arrays.asList(new Tuple3<>(Long.MIN_VALUE, 1L, 0), new Tuple3<>(1L, 9L, 0),
        new Tuple3<>(9L, Long.MAX_VALUE, 0)));

    // BOTH DEGREES with vertex time included
    EXPECTED_BOTH_DEGREES.put(V1,
      EXPECTED_OUT_DEGREES.get(V1));
    EXPECTED_BOTH_DEGREES.put(V2,
      Arrays.asList(new Tuple3<>(0L, 6L, 1), new Tuple3<>(6L, 7L, 2), new Tuple3<>(7L, Long.MAX_VALUE, 1)));
    EXPECTED_BOTH_DEGREES.put(V3,
      Arrays.asList(new Tuple3<>(3L, 4L, 0), new Tuple3<>(4L, 7L, 1), new Tuple3<>(7L, 9L, 0)));
    EXPECTED_BOTH_DEGREES.put(V4,
      Collections.singletonList(new Tuple3<>(4L, 5L, 3)));
    EXPECTED_BOTH_DEGREES.put(V5,
      EXPECTED_IN_DEGREES.get(V5));

    // BOTH DEGREES without vertex time included
    EXPECTED_BOTH_DEGREES_DEF.put(V1,
      EXPECTED_OUT_DEGREES_DEF.get(V1));
    EXPECTED_BOTH_DEGREES_DEF.put(V2,
      Arrays.asList(new Tuple3<>(Long.MIN_VALUE, 0L, 0), new Tuple3<>(0L, 6L, 1), new Tuple3<>(6L, 7L, 2),
        new Tuple3<>(7L, Long.MAX_VALUE, 1)));
    EXPECTED_BOTH_DEGREES_DEF.put(V3,
      Arrays.asList(new Tuple3<>(Long.MIN_VALUE, 4L, 0), new Tuple3<>(4L, 7L, 1),
        new Tuple3<>(7L, Long.MAX_VALUE, 0)));
    EXPECTED_BOTH_DEGREES_DEF.put(V4,
      Arrays.asList(new Tuple3<>(Long.MIN_VALUE, 4L, 0), new Tuple3<>(4L, 5L, 3),
        new Tuple3<>(5L, Long.MAX_VALUE, 0)));
    EXPECTED_BOTH_DEGREES_DEF.put(V5,
      EXPECTED_IN_DEGREES_DEF.get(V5));
  }

  /**
   * The degree type to test.
   */
  @Parameterized.Parameter(0)
  public VertexDegree degreeType;
  /**
   * The flag to enable vertex time consideration.
   */
  @Parameterized.Parameter(1)
  public boolean enableVertexTime;
  /**
   * The expected degree evolution fo the given type.
   */
  @Parameterized.Parameter(2)
  public HashMap<String, List<Tuple3<Long, Long, Integer>>> expectedDegrees;

  /**
   * The temporal graph to test the operator.
   */
  TemporalGraph testGraph;

  /**
   * A mapping from vertex id to label, since vertex id is generated a runtime.
   */
  HashMap<GradoopId, String> idLabelMap = new HashMap<>();

  /**
   * The parameters to test the operator.
   *
   * @return three different vertex degree types with its corresponding expected degree evolution.
   */
  @Parameterized.Parameters(name = "Test degree type {0} with vertex time: {1}")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(
      new Object[] {VertexDegree.IN, true, EXPECTED_IN_DEGREES},
      new Object[] {VertexDegree.OUT, true, EXPECTED_OUT_DEGREES},
      new Object[] {VertexDegree.BOTH, true, EXPECTED_BOTH_DEGREES},
      new Object[] {VertexDegree.IN, false, EXPECTED_IN_DEGREES_DEF},
      new Object[] {VertexDegree.OUT, false, EXPECTED_OUT_DEGREES_DEF},
      new Object[] {VertexDegree.BOTH, false, EXPECTED_BOTH_DEGREES_DEF});
  }

  /**
   * Set up the test graph an create the id-label mapping.
   *
   * @throws Exception in case of an error
   */
  @Before
  public void setUp() throws Exception {
    testGraph = getTestGraphWithValues();
    Collection<Tuple2<GradoopId, String>> idLabelCollection = new HashSet<>();
    testGraph.getVertices().map(v -> new Tuple2<>(v.getId(), v.getLabel()))
      .returns(new TypeHint<Tuple2<GradoopId, String>>() {
      }).output(new LocalCollectionOutputFormat<>(idLabelCollection));
    getExecutionEnvironment().execute();
    for (Tuple2<GradoopId, String> entity : idLabelCollection) {
      idLabelMap.put(entity.f0, entity.f1);
    }
  }

  /**
   * Thest the temporal vertex degree operator with all 3 types of degree.
   *
   * @throws Exception in case of an error
   */
  @Test
  public void testTemporalDegree() throws Exception {
    Collection<Tuple4<GradoopId, Long, Long, Integer>> resultCollection = new HashSet<>();
    HashMap<String, List<Tuple3<Long, Long, Integer>>> labelDegreeMap = new HashMap<>();

    TemporalVertexDegree operator = new TemporalVertexDegree(degreeType, TimeDimension.VALID_TIME);
    operator.setIncludeVertexTime(enableVertexTime);
    final DataSet<Tuple4<GradoopId, Long, Long, Integer>> resultDataSet = testGraph.callForValue(operator);

    resultDataSet.output(new LocalCollectionOutputFormat<>(resultCollection));
    getExecutionEnvironment().execute();

    // Rearrange the result
    for (Tuple4<GradoopId, Long, Long, Integer> resultEntity : resultCollection) {
      if (!labelDegreeMap.containsKey(idLabelMap.get(resultEntity.f0))) {
        labelDegreeMap.put(idLabelMap.get(resultEntity.f0), new ArrayList<>());
      }
      labelDegreeMap.get(idLabelMap.get(resultEntity.f0))
        .add(new Tuple3<>(resultEntity.f1, resultEntity.f2, resultEntity.f3));
    }

    labelDegreeMap.forEach((label, tuple3) -> {
      assertTrue(tuple3.containsAll(expectedDegrees.get(label)));
      assertTrue(expectedDegrees.get(label).containsAll(tuple3));
    });
  }
}
