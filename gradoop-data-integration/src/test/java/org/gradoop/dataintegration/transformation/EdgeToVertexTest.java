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

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Tests for the {@link EdgeToVertex} operator.
 */
public class EdgeToVertexTest extends GradoopFlinkTestBase {

  /**
   * The loader with the graphs used in the tests.
   */
  private final FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
    "(a:VertexA)-[e:edgeToTransform {testProp: 1, testProp2: \"\"}]->(b:VertexB)" +
    "(a)-[e2:edgeToTransform]->(b)" +
    "(a)-[en:anotherEdge]->(b)" +
    "]" +
    "expected [" +
    "(a)-[e]->(b)" +
    "(a)-[e2]->(b)" +
    "(a)-[en]->(b)" +
    "(a)-[:fromSource]->(:VertexFromEdge {testProp: 1, testProp2: \"\"})-[:toTarget]->(b)" +
    "(a)-[:fromSource]->(:VertexFromEdge)-[:toTarget]->(b)" +
    "]");

  /**
   * Test the {@link EdgeToVertex} operator where one new vertex is created.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithVertexCreation() throws Exception {
    UnaryGraphToGraphOperator operator = new EdgeToVertex("edgeToTransform", "VertexFromEdge",
      "fromSource", "toTarget");
    LogicalGraph result = loader.getLogicalGraphByVariable("input").callForGraph(operator);
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    collectAndAssertTrue(result.equalsByElementData(expected));
  }

  /**
   * Test the {@link EdgeToVertex} operator with its first parameter being {@code null}.
   * This should not change anything.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithoutVertexCreation() throws Exception {
    UnaryGraphToGraphOperator operator = new EdgeToVertex(null, "newLabel", "fromSource",
      "toTarget");
    LogicalGraph result = loader.getLogicalGraphByVariable("input").callForGraph(operator);
    LogicalGraph expected = loader.getLogicalGraphByVariable("input");

    collectAndAssertTrue(result.equalsByElementData(expected));
  }

  /**
   * Test if the constructor {@link EdgeToVertex#EdgeToVertex(String, String, String, String)}
   * handles null-values as intended.
   */
  @Test
  public void testForNullParameters() {
    final String notNull = "";
    // Should not throw Exception.
    new EdgeToVertex(null, notNull, notNull, notNull);
    try {
      new EdgeToVertex(null, null, notNull, notNull);
      fail("Second parameter was null.");
    } catch (NullPointerException npe) {
    }
    try {
      new EdgeToVertex(null, notNull, null, notNull);
      fail("Third parameter was null.");
    } catch (NullPointerException npe) {
    }
    try {
      new EdgeToVertex(null, notNull, notNull, null);
      fail("Forth parameter was null.");
    } catch (NullPointerException npe) {
    }
  }
}
