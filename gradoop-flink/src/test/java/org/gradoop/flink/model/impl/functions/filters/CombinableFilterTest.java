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
package org.gradoop.flink.model.impl.functions.filters;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.epgm.ByProperty;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CombinableFilterTest extends GradoopFlinkTestBase {

  private CombinableFilter<Object> alwaysTrue = e -> true;

  private CombinableFilter<Object> alwaysFalse = e -> false;

  private Object testObject = new Object();

  @Test
  public void testAnd() throws Exception {
    assertTrue(alwaysTrue.and(alwaysTrue).filter(testObject));
    assertFalse(alwaysFalse.and(alwaysTrue).filter(testObject));
    assertFalse(alwaysTrue.and(alwaysFalse).filter(testObject));
    assertFalse(alwaysFalse.and(alwaysFalse).filter(testObject));

    assertTrue(new And<>(alwaysTrue, alwaysTrue).filter(testObject));
    assertFalse(new And<>(alwaysFalse, alwaysTrue).filter(testObject));
    assertFalse(new And<>(alwaysTrue, alwaysFalse).filter(testObject));
    assertFalse(new And<>(alwaysFalse, alwaysFalse).filter(testObject));
  }

  @Test
  public void testOr() throws Exception {
    assertTrue(alwaysTrue.or(alwaysTrue).filter(testObject));
    assertTrue(alwaysFalse.or(alwaysTrue).filter(testObject));
    assertTrue(alwaysTrue.or(alwaysFalse).filter(testObject));
    assertFalse(alwaysFalse.or(alwaysFalse).filter(testObject));

    assertTrue(new Or<>(alwaysTrue, alwaysTrue).filter(testObject));
    assertTrue(new Or<>(alwaysFalse, alwaysTrue).filter(testObject));
    assertTrue(new Or<>(alwaysTrue, alwaysFalse).filter(testObject));
    assertFalse(new Or<>(alwaysFalse, alwaysFalse).filter(testObject));
  }

  @Test
  public void testNegate() throws Exception {
    assertTrue(alwaysFalse.negate().filter(testObject));
    assertFalse(alwaysTrue.negate().filter(testObject));

    assertTrue(new Not<>(alwaysFalse).filter(testObject));
    assertFalse(new Not<>(alwaysTrue).filter(testObject));
  }

  @Test
  public void testWithGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();
    loader.appendToDatabaseFromString("expected[" +
      // gender = 'f' and city = 'leipzig'
      "(alice)" +
      // label = 'Tag'
      "(databases)(graphs)(hadoop)" +
      "]");
    LogicalGraph databaseGraph = loader.getLogicalGraph();
    // Filter vertices where
    // (gender = 'f' AND city = 'Leipzig') OR NOT(label = 'Person' OR label = 'Forum')
    FilterFunction<Vertex> vertexFilterFunction =
      new ByProperty<Vertex>("gender", PropertyValue.create("f"))
      .and(new ByProperty<>("city", PropertyValue.create("Leipzig"))).or(
        new ByLabel<>("Person").or(new ByLabel<>("Forum")).negate());
    // Filter edges where
    // label <> 'hasInterest'
    LogicalGraph subgraph = databaseGraph.subgraph(vertexFilterFunction,
      new ByLabel<Edge>("hasInterest").negate(), Subgraph.Strategy.BOTH);
    collectAndAssertTrue(subgraph.equalsByElementIds(loader.getLogicalGraphByVariable("expected")));
  }
}
