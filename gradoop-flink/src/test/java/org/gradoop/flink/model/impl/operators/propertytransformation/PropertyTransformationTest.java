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
package org.gradoop.flink.model.impl.operators.propertytransformation;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.PropertyTransformationFunction;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * A test for {@link PropertyTransformation}, calling the operator and checking if the result is
 * correct.
 */
public class PropertyTransformationTest extends GradoopFlinkTestBase {
  /**
   * The test graphs used for transformation and comparison.
   */
  protected static final String TEST_GRAPH =
      "g0:A  { a : 3000L } [(:A { a : 3000L, b : 3 })-[:a { a : 3000L, b : 3 }]" +
      "->(:B { a: 3000L, c : 23 })]" +
      "g1:B  { a : 3000L } [(:A { a : 3000L, b : 3 })-[:a { a : 3000L, b : 3 }]" +
      "->(:B { a: 3000L, c : 23 })(:C { a : 3000L, b : 3 })-[:b { a : 3000L, b : 3 }]" +
      "->(:D { a: 3000L, c : 23 })]" +
      // full graph transformation
      "g01:A  { a : 3L, a__1 : 3000L } [(:A { a : 3L, a__1 : 3000L, b : 3 })" +
      "-[:a { a : 3L, a__1 : 3000L, b : 3 }]->(:B { a: 3L, a__1 : 3000L, c : 23 })]" +
      // graph head only transformation
      "g02:A { a : 3L, a__1 : 3000L } [(:A { a : 3000L, b : 3 })-[:a { a : 3000L, b : 3 }]" +
      "->(:B { a: 3000L, c : 23 })]" +
      "g12:A { a : 3L } [(:A { a : 3000L, b : 3 })-[:a { a : 3000L, b : 3 }]" +
      "->(:B { a: 3000L, c : 23 })]" +
      "g22:A { dividedA : 3L, a : 3000L } [(:A { a : 3000L, b : 3 })-[:a { a : 3000L, b : 3 }]" +
      "->(:B { a: 3000L, c : 23 })]" +
      // vertex only transformation
      "g03:A { a : 3000L } [(:A { a : 3L, a__1 : 3000L, b : 3 })-[:a { a : 3000L, b : 3 }]" +
      "->(:B { a: 3L, a__1 : 3000L, c : 23 })]" +
      "g13:A { a : 3000L } [(:A { a : 3L, b : 3 })-[:a { a : 3000L, b : 3 }]" +
      "->(:B { a: 3L, c : 23 })]" +
      "g23:A { a : 3000L } [(:A { dividedA : 3L, a : 3000L, b : 3 })" +
      "-[:a { a : 3000L, b : 3 }]->(:B { dividedA: 3L, a : 3000L, c : 23 })]" +
      "g33:A { a : 3000L } [(:A { a : 3L, a__1 : 3000L, b : 3 })-[:a { a : 3000L, b : 3 }]" +
      "->(:B { a : 3000L, c : 23 })]" +
      // edge only transformation
      "g04:A { a : 3000L } [(:A { a : 3000L, b : 3 })-[:a { a : 3L, a__1 : 3000L, b : 3 }]" +
      "->(:B { a: 3000L, c : 23 })]" +
      "g14:A { a : 3000L } [(:A { a : 3000L, b : 3 })-[:a { a : 3L, b : 3 }]" +
      "->(:B { a: 3000L, c : 23 })]" +
      "g24:A { a : 3000L }  [(:A { a : 3000L, b : 3 })" +
      "-[:a { dividedA : 3L, a : 3000L, b : 3 }]->(:B { a: 3000L, c : 23 })]" +
      "g34:B  { a : 3000L } [(:A { a : 3000L, b : 3 })" +
      "-[:a { a : 3L, a__1 : 3000L, b : 3 }]->(:B { a: 3000L, c : 23 })" +
      "(:C { a : 3000L, b : 3 })-[:b { a : 3000L, b : 3 }]" +
      "->(:D { a: 3000L, c : 23 })]";
  /**
   * Simple property transformation that divides a value by 1000.
   */
  protected static PropertyTransformationFunction DIVISION = new DivideBy(1000L);
  /**
   * Executes a property transformation on graphHeads, vertices and edges and checks if the result
   * is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testAllTransformationFunctions() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g01");

    LogicalGraph result = original.callForGraph(
      new PropertyTransformation(
        "a",
        DIVISION,
        DIVISION,
        DIVISION,
        null,
        null,
        true));

    collectAndAssertTrue(result.equalsByData(expected));
  }

  /**
   * Executes a property transformation on graphHeads and checks if the result is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testGHTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g02");

    LogicalGraph result = original.callForGraph(
      new PropertyTransformation(
        "a",
        DIVISION,
        null,
        null,
        null,
        null,
        true));

    collectAndAssertTrue(result.equalsByData(expected));
  }

  /**
   * Executes a property transformation with disabled history on graphHeads and checks if the
   * result is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testGHTransformationWithoutHistory() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g12");

    LogicalGraph result = original.callForGraph(
      new PropertyTransformation(
        "a",
        DIVISION,
        null,
        null));

    collectAndAssertTrue(result.equalsByData(expected));
  }

  /**
   * Executes a property transformation using a new property key on graphHeads and checks if the
   * result is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testGHTransformationNewPropKey() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g22");

    LogicalGraph result = original.callForGraph(
      new PropertyTransformation(
        "a",
        DIVISION,
        null,
        null,
        null,
        "dividedA",
        true));

    collectAndAssertTrue(result.equalsByData(expected));
  }

  /**
   * Executes a property transformation on vertices and checks if the result is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testVertexTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g03");

    LogicalGraph result = original.callForGraph(
      new PropertyTransformation(
        "a",
        null,
        DIVISION,
        null,
        null,
        null,
        true));

    collectAndAssertTrue(result.equalsByData(expected));
  }

  /**
   * Executes a property transformation with disabled history on vertices and checks if the result
   * is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testVertexTransformationWithoutHistory() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g13");

    LogicalGraph result = original.callForGraph(
      new PropertyTransformation(
        "a",
        null,
        DIVISION,
        null));
    
    collectAndAssertTrue(result.equalsByData(expected));
  }

  /**
   * Executes a property transformation using a new property key on vertices and checks if the
   * result is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testVertexTransformationNewPropKey() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g23");

    LogicalGraph result = original.callForGraph(
      new PropertyTransformation(
        "a",
        null,
        DIVISION,
        null,
        null,
        "dividedA",
        true));
    
    collectAndAssertTrue(result.equalsByData(expected));
  }

  /**
   * Executes a property transformation on vertices with a specific label and checks if the
   * result is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testVertexTransformationLabelSpecific() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g33");

    LogicalGraph result = original.callForGraph(
      new PropertyTransformation(
        "a",
        null,
        DIVISION,
        null,
        "A",
        null,
        true));
    
    collectAndAssertTrue(result.equalsByData(expected));
  }

  /**
   * Executes a property transformation on edges and checks if the result is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testEdgeTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g04");

    LogicalGraph result = original.callForGraph(
      new PropertyTransformation(
        "a",
        null,
        null,
        DIVISION,
        null,
        null,
        true));

    collectAndAssertTrue(result.equalsByData(expected));
  }

  /**
   * Executes a property transformation with disabled history on edges and checks if the result
   * is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testEdgeTransformationWithoutHistory() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g14");

    LogicalGraph result = original.callForGraph(
      new PropertyTransformation(
        "a",
        null,
        null,
        DIVISION));

    collectAndAssertTrue(result.equalsByData(expected));
  }

  /**
   * Executes a property transformation using a new property key on edges and checks if the
   * result is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testEdgeTransformationNewPropKey() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g24");

    LogicalGraph result = original.callForGraph(
      new PropertyTransformation(
        "a",
        null,
        null,
        DIVISION,
        null,
        "dividedA",
        true));

    collectAndAssertTrue(result.equalsByData(expected));
  }

  /**
   * Executes a property transformation on edges with a specific label and checks if the
   * result is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testEdgeTransformationLabelSpecific() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g1");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g34");

    LogicalGraph result = original.callForGraph(
      new PropertyTransformation(
        "a",
        null,
        null,
        DIVISION,
        "a",
        null,
        true));

    collectAndAssertTrue(result.equalsByData(expected));
  }
  
  /**
   * Executes a property transformation on graphHeads using the {@link LogicalGraph} object and
   * checks if the result is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testGHTransformationUsingLG() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g12");

    LogicalGraph result = original.transformGraphHeadProperties("a", DIVISION);

    collectAndAssertTrue(result.equalsByData(expected));
  }

  /**
   * Executes a property transformation on vertices using the {@link LogicalGraph} object and
   * checks if the result is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testVertexTransformationUsingLG() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g13");

    LogicalGraph result = original.transformVertexProperties("a", DIVISION);
    
    collectAndAssertTrue(result.equalsByData(expected));
  }

  /**
   * Executes a property transformation on edges using the {@link LogicalGraph} object and
   * checks if the result is correct.
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testEdgeTransformationUsingLG() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);
    
    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g14");

    LogicalGraph result = original.transformEdgeProperties("a", DIVISION);

    collectAndAssertTrue(result.equalsByData(expected));
  }
}