/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.Label;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraphBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphBroadcast;

/**
 * Created by Giacomo Bergami on 25/01/17.
 */
public class FusionUtils {

  /**
   * Recreate another graph which is a copy of the graph x provided as an input
   * @param x   Input graph
   * @return    Copy graph
   */
  public static LogicalGraph recreateGraph(LogicalGraph x) {
    return LogicalGraph
      .fromDataSets(x.getGraphHead(), x.getVertices(), x.getEdges(), x.getConfig());
  }

  /**
   * Given a graph, returns its ID through a DataSet collection
   * @param g   Graph where the id should be returned
   * @return
   */
  public static DataSet<GradoopId> getGraphId(LogicalGraph g) {
    return g.getGraphHead().map(new Id<>());
  }

  /**
   * Given a graph, returns its ID
   * @param g   Graph where from which the id should be returned
   * @return
   */
  public static GradoopId getGraphId2(LogicalGraph g) {
    try {
      return g.getGraphHead().map(new Id<>()).collect().get(0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Filters a collection form a graph dataset performing either an intersection or a difference
   * @param collection      Collection to be filtered
   * @param g         Graph where to verify the containment operation
   * @param inGraph   If the value is true, then perform an intersection, otherwise a difference
   * @param <P>       e.g. either vertices or edges
   * @return
   */
  public static <P extends GraphElement> DataSet<P> areElementsInGraph(DataSet<P> collection,
    LogicalGraph g, boolean inGraph) {
    return collection.filter(inGraph ? new InGraphBroadcast<>() : new NotInGraphBroadcast<>())
      .withBroadcastSet(getGraphId(g), GraphContainmentFilterBroadcast.GRAPH_ID);
  }

  /**
   * Returns a label from a single graph
   * @param g
   * @return
   */
  public static String getGraphLabel(LogicalGraph g) {
    try {
      String toret = g.getGraphHead().map(new Label<>()).collect().get(0);
      return toret;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the properties belonging to a single graph
   * @param g
   * @return
   */
  public static Properties getGraphProperties(LogicalGraph g) {
    try {
      Properties toret =
        g.getGraphHead().map(new org.gradoop.flink.model.impl.functions.epgm.Properties<>())
          .collect().get(0);
      return toret;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
