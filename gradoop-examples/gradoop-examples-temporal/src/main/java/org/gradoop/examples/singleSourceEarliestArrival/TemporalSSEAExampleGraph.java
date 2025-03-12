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
package org.gradoop.examples.singleSourceEarliestArrival;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

/**
 * Class to provide an example graph representing a dump from the citibike rentals of NYC.
 *
 * @see <a href="https://www.citibikenyc.com/system-data">https://www.citibikenyc.com/system-data</a>
 */
public class TemporalSSEAExampleGraph {

  /**
   * ID of the source vertex
   */
  private static GradoopId SRC_VERTEX_ID;

  /**
   * No need for an object of this class.
   */
  private TemporalSSEAExampleGraph() {
  }

  /**
   * Returns the temporal graph instance.
   *
   * @param config the gradoop flink config
   * @return the temporal graph that represents the bike rental example
   */
  public static TemporalGraph getTemporalGraph(GradoopFlinkConfig config) {
    // create loader
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(config);

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
            "]";

    loader.initDatabaseFromString(graph);

    // get LogicalGraph representation of the social network graph
    LogicalGraph networkGraph = loader.getLogicalGraph();
    EPGMVertex srcVertexDouble = loader.getVertexByVariable("v1");
    SRC_VERTEX_ID = srcVertexDouble.getId();

    // transform to temporal graph by extracting time intervals from vertices
    return TemporalGraph.fromGraph(networkGraph)
            .transformEdges(TemporalSSEAExampleGraph::extractTripPeriod);
  }

  public static GradoopId getSrcVertexId() {
    return SRC_VERTEX_ID;
  }

  /**
   * Function to extract the trip period from properties of a vertex. The names of the properties have to be
   * {@code starttime} and {@code stoptime} and their type has to be a String.
   *
   * @param current The current vertex to transform.
   * @param transformed A copy of the current vertex used to create the resulting one.
   * @return The resulting temporal vertex with assigned valid times.
   */
  private static TemporalEdge extractTripPeriod(TemporalEdge current, TemporalEdge transformed) {
    transformed.setLabel(current.getLabel());
    transformed.setProperties(current.getProperties());
    //SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
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
    return transformed;
  }
}
