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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.EPEdgeData;
import org.gradoop.model.EPVertexData;
import org.gradoop.model.store.EPGraphStore;

import java.util.Collection;

public class FlinkGraphStore implements EPGraphStore {

  /**
   * Database graph representing the vertex and edge space.
   */
  private EPGraphCollection database;

  /**
   * Flink execution environment.
   */
  private final ExecutionEnvironment env;

  private FlinkGraphStore(DataSet<Vertex<Long, EPFlinkVertexData>> vertices,
    DataSet<Edge<Long, EPFlinkEdgeData>> edges, ExecutionEnvironment env) {
    this.database =
      new EPGraphCollection(Graph.fromDataSet(vertices, edges, env), null, env);
    this.env = env;
  }

  public static EPGraphStore fromCollection(Collection<EPFlinkVertexData> vertices,
    Collection<EPFlinkEdgeData> edges, ExecutionEnvironment env) {
    DataSet<EPFlinkVertexData> epgmVertexSet = env.fromCollection(vertices);
    DataSet<EPFlinkEdgeData> epgmEdgeSet = env.fromCollection(edges);


    DataSet<Vertex<Long, EPFlinkVertexData>> vertexDataSet = null;
    DataSet<Edge<Long, EPFlinkEdgeData>> edgeDataSet = null;

    if (epgmVertexSet != null) {
      vertexDataSet = epgmVertexSet.map(new VerticesConverter());
      edgeDataSet = epgmEdgeSet.map(new EdgesConverter());
    }
    return new FlinkGraphStore(vertexDataSet, edgeDataSet, env);
  }

  @Override
  public EPGraph getDatabaseGraph() {
    return database.getGraph();
  }

  @Override
  public EPGraphCollection getCollection() {
    return database;
  }

  @Override
  public EPGraph getGraph(Long graphID) {
    return null;
  }

  /**
   * Takes an EPGM vertex and converts it into a flink vertex.
   */
  public static class VerticesConverter implements
    MapFunction<EPFlinkVertexData, Vertex<Long, EPFlinkVertexData>> {

    @Override
    public Vertex<Long, EPFlinkVertexData> map(EPFlinkVertexData value) throws
      Exception {
      Vertex<Long, EPFlinkVertexData> vertex = new Vertex<>();
      vertex.setId(value.getId());
      vertex.setValue(value);
      return vertex;
    }
  }

  /**
   * Takes an EPGM vertex and produces a collection of flink edges based on
   * its outgoing edges.
   */
  public static class EdgesConverter implements
    MapFunction<EPFlinkEdgeData, Edge<Long, EPFlinkEdgeData>> {

    @Override
    public Edge<Long, EPFlinkEdgeData> map(EPFlinkEdgeData value) throws
      Exception {
      Edge<Long, EPFlinkEdgeData> e = new Edge<>();
      e.setSource(value.getSourceVertex());
      e.setTarget(value.getTargetVertex());
      e.setValue(value);
      return e;
    }
  }
}
