/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;
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
   * Given g...
   * @param g   Graph where the id should be returned
   * @return    returns its ID through a DataSet collection
   */
  public static DataSet<GradoopId> getGraphId(LogicalGraph g) {
    return g.getGraphHead().map(new Id<>());
  }

  /**
   * Given g...
   * @param g   Graph where from which the id should be returned
   * @return    returns its ID
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
   * @return          The filtered collection
   */
  public static <P extends GraphElement> DataSet<P> areElementsInGraph(DataSet<P> collection,
    LogicalGraph g, boolean inGraph) {
    return collection.filter(inGraph ? new InGraphBroadcast<>() : new NotInGraphBroadcast<>())
      .withBroadcastSet(getGraphId(g), GraphContainmentFilterBroadcast.GRAPH_ID);
  }

}
