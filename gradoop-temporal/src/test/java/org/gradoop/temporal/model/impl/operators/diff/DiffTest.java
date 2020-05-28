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
package org.gradoop.temporal.model.impl.operators.diff;

import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.functions.predicates.AsOf;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.Test;

/**
 * Test for the temporal diff operator.
 */
public class DiffTest extends TemporalGradoopTestBase {
  /**
   * Test the temporal diff operator on an example graph.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testDiffOnGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();
    loader.appendToDatabaseFromString("expected:Forum[" +
      "(gpse:Forum {title: \"Graph Processing\", _diff: 0})" +
      "(davee:Person {name: \"Dave\", gender: \"m\", city: \"Dresden\", age: 40," +
      "__valFrom: 1543700000000L, _diff: 1})" +
      "(carole:Person {name : \"Carol\", gender : \"f\", city : \"Dresden\", age : 30," +
      "__valFrom: 1543600000000L, _diff: 0})" +
      "(gpse)-[:hasMember{ __valFrom: 1543600000000L, __valTo: 1543800000000L, _diff: -1}]->" +
      "(davee) (gpse)-[:hasMember {_diff: 0}]->(carole)" +
      "-[:knows {since : 2014 , __valFrom : 1543700000000L, _diff: 1}]->(davee)" +
      "]");
    LogicalGraph inputGraphEpgm = loader.getLogicalGraphByVariable("g3");
    TemporalGraph temporalGraph = toTemporalGraphWithDefaultExtractors(inputGraphEpgm);
    TemporalGraph result = temporalGraph.diff(new AsOf(1543600000000L), new AsOf(1543800000000L));

    collectAndAssertTrue(toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("expected"))
      .equalsByData(result));
  }
}
