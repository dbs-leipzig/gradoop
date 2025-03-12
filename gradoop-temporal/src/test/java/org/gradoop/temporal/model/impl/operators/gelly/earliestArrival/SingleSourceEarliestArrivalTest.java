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
package org.gradoop.temporal.model.impl.operators.gelly.earliestArrival;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Test;

/**
 * Test each case of Single Source Earliest Arrival
 */
public class SingleSourceEarliestArrivalTest extends TemporalGradoopTestBase {

  String graph = "input:test[" +
          "(v1 {id:1})" +
          "(v2 {id:2})" +
          "(v3 {id:3})" +
          "(v4 {id:4})" +
          "(v5 {id:5})" +
          "(v1)-[e1 {starttime: 0L, endtime: 3L}]->(v2)" +
          "(v2)-[e2 {starttime: 2L, endtime: 7L}]->(v4)" +
          "(v2)-[e3 {starttime: 4L, endtime: 5L}]->(v3)" +
          "(v3)-[e4 {starttime: 6L, endtime: 8L}]->(v4)" +
          "(v4)-[e5 {starttime: 8L, endtime: 10L}]->(v5)" +
          "(v3)-[e6 {starttime: 7L, endtime: 12L}]->(v5)" +
          "]" +
          "intervalOverlap:test[" +
          "(v6 {SSEA:0L,id:1})" +
          "(v7 {SSEA:0L,id:2})" +
          "(v8 {SSEA:4L,id:3})" +
          "(v9 {SSEA:2L,id:4})" +
          "(v10 {SSEA:7L,id:5})" +
          "(v6)-[e7 {starttime: 0L, endtime: 3L}]->(v7)" +
          "(v7)-[e8 {starttime: 2L, endtime: 7L}]->(v9)" +
          "(v7)-[e9 {starttime: 4L, endtime: 5L}]->(v8)" +
          "(v8)-[e10 {starttime: 6L, endtime: 8L}]->(v9)" +
          "(v9)-[e11 {starttime: 8L, endtime: 10L}]->(v10)" +
          "(v8)-[e12 {starttime: 7L, endtime: 12L}]->(v10)" +
          "]" +
          "noInterval:test[" +
          "(v11 {SSEA:0L,id:1})" +
          "(v12 {SSEA:0L,id:2})" +
          "(v13 {SSEA:4L,id:3})" +
          "(v14 {SSEA:2L,id:4})" +
          "(v15 {SSEA:7L,id:5})" +
          "(v11)-[e13 {starttime: 0L, endtime: 3L}]->(v12)" +
          "(v12)-[e14 {starttime: 2L, endtime: 7L}]->(v14)" +
          "(v12)-[e15 {starttime: 4L, endtime: 5L}]->(v13)" +
          "(v13)-[e16 {starttime: 6L, endtime: 8L}]->(v14)" +
          "(v14)-[e17 {starttime: 8L, endtime: 10L}]->(v15)" +
          "(v13)-[e18 {starttime: 7L, endtime: 12L}]->(v15)" +
          "]" +
          "intervalNoOverlap:test[" +
          "(v16 {SSEA:0L,id:1})" +
          "(v17 {SSEA:3L,id:2})" +
          "(v18 {SSEA:5L,id:3})" +
          "(v19 {SSEA:8L,id:4})" +
          "(v20 {SSEA:10L,id:5})" +
          "(v16)-[e19 {starttime: 0L, endtime: 3L}]->(v17)" +
          "(v17)-[e20 {starttime: 2L, endtime: 7L}]->(v19)" +
          "(v17)-[e21 {starttime: 4L, endtime: 5L}]->(v18)" +
          "(v18)-[e22 {starttime: 6L, endtime: 8L}]->(v19)" +
          "(v19)-[e23 {starttime: 8L, endtime: 10L}]->(v20)" +
          "(v18)-[e24 {starttime: 7L, endtime: 12L}]->(v20)" +
          "]";


  /**
   * Test without interval
   * @throws Exception
   */
  @Test
  public void testByDataNoInterval() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(graph);
    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    GradoopId srcVertex = loader.getVertexByVariable("v1").getId();
    TemporalGraph in = TemporalGraph.fromGraph(input)
            .transformEdges(SingleSourceEarliestArrivalTest::extractTripPeriod);

    TemporalGraph noIntervalOutput = in.singleSourceEarliestArrival(srcVertex, 100, "SSEA", false, 0L, true);

    LogicalGraph inputNoInterval = loader.getLogicalGraphByVariable("noInterval");
    TemporalGraph noIntervalResult = TemporalGraph.fromGraph(inputNoInterval)
            .transformEdges(SingleSourceEarliestArrivalTest::extractTripPeriod);

    collectAndAssertTrue(noIntervalOutput.equalsByData(noIntervalResult));
  }

  /**
   * Test with interval and without overlap
   * @throws Exception
   */
  @Test
  public void testbyDataIntervalNoOverlap() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(graph);
    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    GradoopId srcVertex = loader.getVertexByVariable("v1").getId();
    TemporalGraph in = TemporalGraph.fromGraph(input)
            .transformEdges(SingleSourceEarliestArrivalTest::extractTripPeriod);
    TemporalGraph intervalNoOverlapOutput = in.singleSourceEarliestArrival(srcVertex, 100, "SSEA", true, 0L, false);

    LogicalGraph inputNoOverlapInterval = loader.getLogicalGraphByVariable("intervalNoOverlap");
    TemporalGraph intervalNoOverlapResult = TemporalGraph.fromGraph(inputNoOverlapInterval)
            .transformEdges(SingleSourceEarliestArrivalTest::extractTripPeriod);

    collectAndAssertTrue(intervalNoOverlapOutput.equalsByData(intervalNoOverlapResult));
  }

  /**
   * Test with interval and overlap
   * @throws Exception
   */
  @Test
  public void testbyDataIntervalOverlap() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(graph);
    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    GradoopId srcVertex = loader.getVertexByVariable("v1").getId();
    TemporalGraph in = TemporalGraph.fromGraph(input)
            .transformEdges(SingleSourceEarliestArrivalTest::extractTripPeriod);

    TemporalGraph intervalOverlapOutput = in.singleSourceEarliestArrival(srcVertex, 100, "SSEA", true, 0L, true);

    LogicalGraph inputIntervalOverlap = loader.getLogicalGraphByVariable("intervalOverlap");
    TemporalGraph intervalOverlapResult = TemporalGraph.fromGraph(inputIntervalOverlap)
            .transformEdges(SingleSourceEarliestArrivalTest::extractTripPeriod);

    collectAndAssertTrue(intervalOverlapOutput.equalsByData(intervalOverlapResult));

  }


  /**
   * Extract Trip Period from Edge
   * @param current Temporal Edge
   * @param transformed Temporal Edge
   * @return  transformed Edge
   */
  private static TemporalEdge extractTripPeriod(TemporalEdge current, TemporalEdge transformed) {
    transformed.setLabel(current.getLabel());
    transformed.setProperties(current.getProperties());
    //SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    try {
      String startTime = "starttime";
      if (current.hasProperty(startTime)) {
        transformed.setValidFrom(current.getPropertyValue("starttime").getLong());
        transformed.removeProperty(startTime);
        String stopTime = "endtime";
        if (current.hasProperty(stopTime)) {
          transformed.setValidTo(current.getPropertyValue("endtime").getLong());
          transformed.removeProperty(stopTime);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Can not parse time.");
    }
    return transformed;
  }
}
