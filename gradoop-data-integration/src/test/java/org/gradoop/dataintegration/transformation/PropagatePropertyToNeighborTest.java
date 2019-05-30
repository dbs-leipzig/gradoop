/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.transformation;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.dataintegration.transformation.impl.PropertyTransformation;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Tests for the {@link PropagatePropertyToNeighbor} operator.
 */
public class PropagatePropertyToNeighborTest extends GradoopFlinkTestBase {

  /**
   * A comparator ordering property values by type first.
   * This will effectively be able to compare any property value.
   * Whether this order makes any sense is not relevant for this test, it is just used to make sure
   * that two list-typed properties have the same order.
   */
  private static final Comparator<PropertyValue> byTypeFirst = Comparator
    .comparing((PropertyValue pv) -> pv.getType().getSimpleName())
    .thenComparing(Comparator.naturalOrder());

  /**
   * The loader with the graphs used in this test.
   */
  private FlinkAsciiGraphLoader loader = getLoaderFromString(
    "input1:test[" + "(s1:Source {p1: 1, p2: 1.1d})-[e1:edge1]->(t:Target {t: 0})" +
      "(s2:Source {p1: \"\"})-[e2:edge2]->(t)" +
      "(s1)-[e12:edge1]->(t2:Target2 {t: 0})" +
      "(s2)-[e22:edge2]->(t2)" +
      "]" +
      "input2:test [" +
      "(v:Vertex {t: 1})-->(v)" +
      "]");

  /**
   * Test the operator propagating a property to two vertices.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testPropagateDirected() throws Exception {
    LogicalGraph input = loader.getLogicalGraphByVariable("input1");
    UnaryGraphToGraphOperator operator = new PropagatePropertyToNeighbor("Source", "p1", "t");
    // We have to update the vertices manually because the ascii loader does not support lists.
    LogicalGraph expected = input.transformVertices((v, c) -> {
      if (!v.getLabel().equals("Target") && !v.getLabel().equals("Target2")) {
        return v;
      }
      v.setProperty("t", Arrays.asList(PropertyValue.create(1L), PropertyValue.create("")));
      return v;
    }).callForGraph(new PropertyTransformation<>("t", pv -> pv,
      PropagatePropertyToNeighborTest::orderListProperty, pv -> pv));
    LogicalGraph result = input.callForGraph(operator)
      .callForGraph(new PropertyTransformation<>("t", pv -> pv,
      PropagatePropertyToNeighborTest::orderListProperty, pv -> pv));
    collectAndAssertTrue(expected.equalsByData(result));
  }

  /**
   * Test the operator propagating along only certain edge labels.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testPropagateAlongCertainEdges() throws Exception {
    LogicalGraph input = loader.getLogicalGraphByVariable("input1");
    UnaryGraphToGraphOperator operator = new PropagatePropertyToNeighbor("Source", "p1", "t",
      Collections.singleton("edge1"), null);
    LogicalGraph expected = input.transformVertices((v, c) -> {
      if (!v.getLabel().equals("Target") && !v.getLabel().equals("Target2")) {
        return v;
      }
      v.setProperty("t", Collections.singletonList(PropertyValue.create(1L)));
      return v;
    });
    LogicalGraph result = input.callForGraph(operator);
    collectAndAssertTrue(expected.equalsByData(result));
  }

  /**
   * Test the operator propagating only to vertices with a certain label.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testPropagateToCertainVertices() throws Exception {
    LogicalGraph input = loader.getLogicalGraphByVariable("input1");
    UnaryGraphToGraphOperator operator = new PropagatePropertyToNeighbor("Source", "p1", "t",
      null, Collections.singleton("Target"));
    LogicalGraph expected = input.transformVertices((v, c) -> {
      if (!v.getLabel().equals("Target")) {
        return v;
      }
      v.setProperty("t", Arrays.asList(PropertyValue.create(1L), PropertyValue.create("")));
      return v;
    }).callForGraph(new PropertyTransformation<>("t", pv -> pv,
      PropagatePropertyToNeighborTest::orderListProperty, pv -> pv));
    LogicalGraph result = input.callForGraph(operator)
      .callForGraph(new PropertyTransformation<>("t", pv -> pv,
      PropagatePropertyToNeighborTest::orderListProperty, pv -> pv));
    collectAndAssertTrue(expected.equalsByData(result));
  }

  /**
   * Test the operator propagating only to vertices with a certain label and only along edges of
   * a certain label.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testPropagateToCertainVerticesAlongCertainEdges() throws Exception {
    LogicalGraph input = loader.getLogicalGraphByVariable("input1");
    UnaryGraphToGraphOperator operator = new PropagatePropertyToNeighbor("Source", "p1", "t",
      Collections.singleton("edge1"), Collections.singleton("Target"));
    LogicalGraph expected = input.transformVertices((v, c) -> {
      if (!v.getLabel().equals("Target")) {
        return v;
      }
      v.setProperty("t", Collections.singletonList(PropertyValue.create(1L)));
      return v;
    });
    LogicalGraph result = input.callForGraph(operator);
    collectAndAssertTrue(expected.equalsByData(result));
  }

  /**
   * Test if the operator works correctly for loops.
   * This will also check if using the same property key for reading and writing values works.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testPropagateInLoops() throws Exception {
    LogicalGraph input = loader.getLogicalGraphByVariable("input2");
    UnaryGraphToGraphOperator operator = new PropagatePropertyToNeighbor("Vertex", "t", "t");
    LogicalGraph expected = input.transformVertices((v, c) -> {
      v.setProperty("t", Collections.singletonList(PropertyValue.create(1L)));
      return v;
    });
    LogicalGraph result = input.callForGraph(operator);
    collectAndAssertTrue(expected.equalsByData(result));
  }

  /**
   * Order a list-type property. This is used to make sure that two lists are equal except
   * for the order of their elements.
   *
   * @param list The property.
   * @return The ordered property.
   */
  private static PropertyValue orderListProperty(PropertyValue list) {
    if (!list.isList()) {
      return list;
    }
    List<PropertyValue> theList = list.getList();
    theList.sort(byTypeFirst);
    return PropertyValue.create(theList);
  }
}
