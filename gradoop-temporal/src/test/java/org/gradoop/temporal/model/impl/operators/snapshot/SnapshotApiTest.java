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
package org.gradoop.temporal.model.impl.operators.snapshot;

import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Test of the snapshot API functions of {@link TemporalGraph}.
 */
public class SnapshotApiTest extends TemporalGradoopTestBase {

  /**
   * The loader instance to hold a graph for testing.
   */
  private FlinkAsciiGraphLoader loader;

  /**
   * Setup method to initialize the loader instance.
   *
   * @throws IOException on failure
   */
  @BeforeMethod
  public void setUp() throws IOException {
    loader = getTemporalSocialNetworkLoader();
  }

  /**
   * Test the method {@link TemporalGraph#asOf(long)}.
   *
   * @throws Exception on failure
   */
  @Test
  public void testAsOf() throws Exception {
    String resultGraph = "expected[(alice)-[akb]->(bob)-[bka]->(alice)]";
    loader.appendToDatabaseFromString(resultGraph);

    TemporalGraph input = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("g0"));

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    TemporalGraph result = input.asOf(1543600000000L);
    collectAndAssertTrue(result.toLogicalGraph().equalsByElementData(expected));
  }

  /**
   * Test the method {@link TemporalGraph#fromTo(long, long)}.
   *
   * @throws Exception on failure
   */
  @Test
  public void testFromTo() throws Exception {
    String resultGraph = "expected[(alice)-[akb]->(bob)-[bka]->(alice)]";
    loader.appendToDatabaseFromString(resultGraph);

    TemporalGraph input = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("g0"));

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    TemporalGraph result = input.fromTo(1543500000000L, 1543800000000L);
    collectAndAssertTrue(result.toLogicalGraph().equalsByElementData(expected));
  }

  /**
   * Test the method {@link TemporalGraph#between(long, long)}.
   *
   * @throws Exception on failure
   */
  @Test
  public void testBetween() throws Exception {
    String resultGraph = "expected[(eve)-[eka]->(alice)-[akb]->(bob)-[bka]->(alice)]";
    loader.appendToDatabaseFromString(resultGraph);

    TemporalGraph input = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("g0"));

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    TemporalGraph result = input.between(1543500000000L, 1543800000000L);
    collectAndAssertTrue(result.toLogicalGraph().equalsByElementData(expected));
  }

  /**
   * Test the method {@link TemporalGraph#containedIn(long, long)}.
   *
   * @throws Exception on failure
   */
  @Test
  public void testContainedIn() throws Exception {
    String g4 = "g4[(t1:A {__valFrom: 1543700000000L, __valTo: 1543900000000L})-->" +
      "(t2:B {__valFrom: 1543700000000L})-->(t3:C {__valTo: 1543800000000L})-->" +
      "(t4:D {__valFrom: 1543600000000L, __valTo: 1543800000000L})-->" +
      "(t5:F {__valFrom: 1543700000000L, __valTo: 1544000000000L})]";
    loader.appendToDatabaseFromString(g4);

    String resultGraph = "expected[(t1)]";
    loader.appendToDatabaseFromString(resultGraph);

    TemporalGraph input = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("g4"));

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    TemporalGraph result = input.containedIn(1543700000000L, 1543900000000L);
    collectAndAssertTrue(result.toLogicalGraph().equalsByElementData(expected));
  }

  /**
   * Test the method {@link TemporalGraph#validDuring(long, long)}.
   *
   * @throws Exception on failure
   */
  @Test
  public void testValidDuring() throws Exception {
    String resultGraph = "expected[(gps)-[:hasMember]->(carol)]";
    loader.appendToDatabaseFromString(resultGraph);

    TemporalGraph input = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("g3"));

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    TemporalGraph result = input.validDuring(1543600000000L, 1543800000000L);
    collectAndAssertTrue(result.toLogicalGraph().equalsByElementData(expected));
  }

  /**
   * Test the method {@link TemporalGraph#createdIn(long, long)}.
   *
   * @throws Exception on failure
   */
  @Test
  public void testCreatedIn() throws Exception {
    String resultGraph = "expected[(eve)(bob)]";
    loader.appendToDatabaseFromString(resultGraph);

    TemporalGraph input = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("g0"));

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    TemporalGraph result = input.createdIn(1543500000000L, 1543800000000L);
    collectAndAssertTrue(result.toLogicalGraph().equalsByElementData(expected));
  }

  /**
   * Test the method {@link TemporalGraph#deletedIn(long, long)}.
   *
   * @throws Exception on failure
   */
  @Test
  public void testDeletedIn() throws Exception {
    String g4 = "g4[(t1:A {__valFrom: 1543700000000L, __valTo: 1543900000000L})-->" +
      "(t2:B {__valFrom: 1543700000000L})-->(t3:C {__valTo: 1543800000000L})-->" +
      "(t4:D {__valFrom: 1543600000000L, __valTo: 1543800000000L})-->" +
      "(t5:F {__valFrom: 1543700000000L, __valTo: 1544000000000L})]";
    loader.appendToDatabaseFromString(g4);

    String resultGraph = "expected[(t1)]";
    loader.appendToDatabaseFromString(resultGraph);

    TemporalGraph input = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("g4"));

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    TemporalGraph result = input.deletedIn(1543900000000L, 1543900000000L);
    collectAndAssertTrue(result.toLogicalGraph().equalsByElementData(expected));
  }
}
