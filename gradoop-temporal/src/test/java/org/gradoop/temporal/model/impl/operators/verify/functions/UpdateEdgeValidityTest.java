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
package org.gradoop.temporal.model.impl.operators.verify.functions;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test for the {@link UpdateEdgeValidity} function used by the temporal verify operator.
 */
public class UpdateEdgeValidityTest extends TemporalGradoopTestBase {

  /**
   * A list storing the join results.
   */
  private List<TemporalEdge> result;

  /**
   * The operator to test.
   */
  private UpdateEdgeValidity operator;

  /**
   * The output for the operator.
   */
  private Collector<TemporalEdge> out;

  /**
   * A test vertex.
   */
  private TemporalVertex vertex;

  /**
   * Set up this test.
   */
  @BeforeMethod
  public void setUp() {
    result = new ArrayList<>();
    out = new ListCollector<>(result);
    operator = new UpdateEdgeValidity();
    vertex = getVertexFactory().createVertex();
    vertex.setValidFrom(asMillis("2020.02.02 12:00:00.000"));
    vertex.setValidTo(asMillis("2020.02.03 12:00:00.000"));
  }

  /**
   * Test if non-overlapping edges will be removed properly.
   */
  @Test
  public void testRemoveNonOverlapping() {
    TemporalEdge edge = getEdgeFactory().createEdge(GradoopId.NULL_VALUE, GradoopId.NULL_VALUE);
    // Edge is valid after the vertex.
    edge.setValidFrom(asMillis("2020.02.03 12:00:00.000"));
    edge.setValidTo(asMillis("2020.02.04 12:00:00.000"));
    operator.join(edge, vertex, out);
    assertTrue(result.isEmpty());
    // Edge is valid before the vertex.
    edge.setValidFrom(asMillis("2020.02.01 12:00:00.000"));
    edge.setValidTo(asMillis("2020.02.02 11:59:59.999"));
    operator.join(edge, vertex, out);
    assertTrue(result.isEmpty());
  }

  /**
   * Test if the {@code validFrom} field is updated accordingly.
   */
  @Test
  public void testUpdateValidFrom() {
    TemporalEdge edge = getEdgeFactory().createEdge(GradoopId.NULL_VALUE, GradoopId.NULL_VALUE);
    final long validTo = asMillis("2020.02.02 13:00:00.000");
    edge.setValidFrom(asMillis("2019.12.31 00:00:00.000"));
    edge.setValidTo(validTo);
    operator.join(edge, vertex, out);
    assertEquals(result.size(), 1);
    TemporalEdge resultEdge = result.get(0);
    assertEquals(resultEdge.getValidFrom(), vertex.getValidFrom());
    assertEquals((long) resultEdge.getValidTo(), validTo);
  }

  /**
   * Test if the {@code validTo} field is updated accordingly.
   */
  @Test
  public void testUpdateValidTo() {
    TemporalEdge edge = getEdgeFactory().createEdge(GradoopId.NULL_VALUE, GradoopId.NULL_VALUE);
    final long validFrom = asMillis("2020.02.02 13:00:00.000");
    edge.setValidFrom(validFrom);
    edge.setValidTo(asMillis("2020.03.01 12:00:00.000"));
    operator.join(edge, vertex, out);
    assertEquals(result.size(), 1);
    TemporalEdge resultEdge = result.get(0);
    assertEquals((long) resultEdge.getValidFrom(), validFrom);
    assertEquals(resultEdge.getValidTo(), vertex.getValidTo());
  }

  /**
   * Test if both fields are updated accordingly.
   */
  @Test
  public void testUpdateBoth() {
    TemporalEdge edge = getEdgeFactory().createEdge(GradoopId.NULL_VALUE, GradoopId.NULL_VALUE);
    edge.setValidFrom(asMillis("2020.01.01 12:00:00.000"));
    edge.setValidTo(asMillis("2020.03.01 12:00:00.000"));
    operator.join(edge, vertex, out);
    assertEquals(result.size(), 1);
    TemporalEdge resultEdge = result.get(0);
    assertEquals(resultEdge.getValidFrom(), vertex.getValidFrom());
    assertEquals(resultEdge.getValidTo(), vertex.getValidTo());
  }
}
