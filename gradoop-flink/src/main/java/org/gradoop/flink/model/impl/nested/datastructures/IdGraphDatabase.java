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

package org.gradoop.flink.model.impl.nested.datastructures;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.nested.datastructures.functions.AssociateElementToIdAndGraph;
import org.gradoop.flink.model.impl.nested.datastructures.functions.ExceptGraphHead;
import org.gradoop.flink.model.impl.nested.datastructures.functions.SelfId;
import org.gradoop.flink.model.impl.nested.datastructures.functions.VertexToGraphHead;
import org.gradoop.flink.model.impl.nested.operators.nesting.functions.UpdateEdges;
import org.gradoop.flink.model.impl.nested.operators.nesting.functions.UpdateVertices;

/**
 * Defines a graph collection only by using the graph id elements (and hence, reducing the
 * exchanged data volume)
 */
public class IdGraphDatabase {

  /**
   * Ids corresponding to the graph heads
   */
  private DataSet<GradoopId> graphHeads;

  /**
   * Pairs of graph heads' ids and vertices' ids belonging to a specific graph head
   */
  private DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToVertex;

  /**
   * Pairs of graph heads' ids and edges' ids belonging to a specific graph head
   */
  private DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToEdge;

  /**
   * Creating an instance of the graph database by just using the elements' ids
   * @param graphHeads          The heads defining the component at the first level of annidation
   * @param graphHeadToVertex   The vertices defining the components at the intermediately down
   *                            level
   * @param graphHeadToEdge     The edges appearing between each possible level
   */
  public IdGraphDatabase(DataSet<GradoopId> graphHeads,
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToVertex,
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToEdge) {
    this.graphHeads = graphHeads;
    this.graphHeadToVertex = graphHeadToVertex;
    this.graphHeadToEdge = graphHeadToEdge;
  }

  /**
   * Extracts the id from the normalized graph
   * @param logicalGraph  Graph where to extract the ids from
   */
  public IdGraphDatabase(NormalizedGraph logicalGraph) {
    this.graphHeads = logicalGraph.getGraphHeads().map(new Id<>());
    initVertices(logicalGraph.getVertices(), logicalGraph.getEdges());
  }

  /**
   * Extracts the id from the normalized graph
   * @param logicalGraph  Graph where to extract the ids from
   */
  public IdGraphDatabase(LogicalGraph logicalGraph) {
    this.graphHeads = logicalGraph.getGraphHead().map(new Id<>());
    initVertices(logicalGraph.getVertices(), logicalGraph.getEdges());
  }

  /**
   * Extracts the id from the normalized graph
   * @param logicalGraph  Graph where to extract the ids from
   */
  public IdGraphDatabase(GraphCollection logicalGraph) {
    this.graphHeads = logicalGraph.getGraphHeads().map(new Id<>());
    initVertices(logicalGraph.getVertices(), logicalGraph.getEdges());
  }

  /**
   * Associates the elements' id to the respective values
   * @param dataLake  Lake containing all the values associated to the ids
   * @return          Istantiated collection
   */
  public GraphCollection asGraphCollection(LogicalGraph dataLake) {
    DataSet<Vertex> vertices = graphHeadToVertex
      .coGroup(dataLake.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices());

    DataSet<Edge> edges = graphHeadToEdge
      .coGroup(dataLake.getEdges())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateEdges());

    DataSet<GraphHead> heads = getActualGraphHeads(dataLake);

    return GraphCollection.fromDataSets(heads, vertices, edges, dataLake.getConfig());
  }

  /**
   * Associates the elements' id to the respective values
   * @param dataLake  Lake containing all the values associated to the ids
   * @return          Istantiated dataset
   */
  public LogicalGraph asLogicalGraph(LogicalGraph dataLake) {
    DataSet<Vertex> vertices = graphHeadToVertex
      .coGroup(dataLake.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices());

    DataSet<Edge> edges = graphHeadToEdge
      .coGroup(dataLake.getEdges())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateEdges());

    DataSet<GraphHead> heads = getActualGraphHeads(dataLake);

    return LogicalGraph.fromDataSets(heads, vertices, edges, dataLake.getConfig());
  }

  /**
   * Recreates the NormalizedGraph of the informations stored within
   * the IdGraphDatabase through the DataLake
   * @param dataLake  Element containing all the required and necessary
   *                  informations
   * @return          Actual values
   */
  public NormalizedGraph asNormalizedGraph(DataLake dataLake) {
    return asNormalizedGraph(dataLake.asNormalizedGraph());
  }

  /**
   * Recreates the NormalizedGraph of the informations stored within
   * the IdGraphDatabase through the DataLake
   * @param dataLake  Element containing all the required and necessary
   *                  informations
   * @return          Actual values
   */
  public NormalizedGraph asNormalizedGraph(NormalizedGraph dataLake) {
    DataSet<Vertex> vertices = graphHeadToVertex
      .coGroup(dataLake.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices());

    DataSet<Edge> edges = graphHeadToEdge
      .coGroup(dataLake.getEdges())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateEdges());

    DataSet<GraphHead> heads = getActualGraphHeads(dataLake);

    return new NormalizedGraph(heads, vertices, edges, dataLake.getConfig());
  }

  /**
   * Recreates the NormalizedGraph of the informations stored within
   * the IdGraphDatabase through the DataLake
   * @param dataLake  Element containing all the required and necessary
   *                  informations
   * @return          Actual values
   */
  public NormalizedGraph asNormalizedGraph(LogicalGraph dataLake) {
    DataSet<Vertex> vertices = graphHeadToVertex
      .coGroup(dataLake.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices());

    DataSet<Edge> edges = graphHeadToEdge
      .coGroup(dataLake.getEdges())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateEdges());

    DataSet<GraphHead> heads = getActualGraphHeads(dataLake);

    return new NormalizedGraph(heads, vertices, edges, dataLake.getConfig());
  }

  /**
   * Returns…
   * @return  the graph heads' ids
   */
  public DataSet<GradoopId> getGraphHeads() {
    return graphHeads;
  }

  /**
   * Instantiates the GraphHeads using the LogicalGraph as a primary source
   * @param dataLake  primary source
   * @return          GraphHeads
   */
  public DataSet<GraphHead> getActualGraphHeads(LogicalGraph dataLake) {
    return graphHeads
      .join(dataLake.getVertices())
      .where(new SelfId()).equalTo(new Id<>())
      .with(new VertexToGraphHead());
  }

  /**
   * Instantiates the GraphHeads using the NormalizedGraph as a primary source
   * @param dataLake  primary source
   * @return          GraphHeads
   */
  public DataSet<GraphHead> getActualGraphHeads(NormalizedGraph dataLake) {
    return graphHeads
      .leftOuterJoin(dataLake.getVertices())
      .where(new SelfId()).equalTo(new Id<>())
      .with(new VertexToGraphHead());
  }

  /**
   * Converts the GraphHeads to vertices
   * @param dataLake  primary source
   * @return          GraphHeads
   */
  public DataSet<Vertex> getActualGraphHeadsAsVertices(LogicalGraph dataLake) {
    return graphHeads
      .join(dataLake.getVertices())
      .where(new SelfId()).equalTo(new Id<>())
      .with(new RightSide<>());
  }

  /**
   * Retuns…
   * @return  the association between the graph heads and the vertices
   */
  public DataSet<Tuple2<GradoopId, GradoopId>> getGraphHeadToVertex() {
    return graphHeadToVertex;
  }

  /**
   * Returns…
   * @return  the association between the graph heads and the edges
   */
  public DataSet<Tuple2<GradoopId, GradoopId>> getGraphHeadToEdge() {
    return graphHeadToEdge;
  }

  /**
   * Updates the edges' id sets with some new ids
   * @param tuple2DataSet Elements to be added
   */
  public void addNewEdges(DataSet<Tuple2<GradoopId, GradoopId>> tuple2DataSet) {
    graphHeadToEdge = graphHeadToEdge.union(tuple2DataSet);
  }

  /**
   * Updates the vertices' id sets with some new ids
   * @param cross Elements to be added
   */
  public void addNewVertices(DataSet<Tuple2<GradoopId, GradoopId>> cross) {
    graphHeadToVertex = graphHeadToVertex.union(cross);
  }

  /**
   * Initializes the vertices and the edges with a similar procedure
   * @param v Vertices
   * @param e Edges
   */
  private void initVertices(DataSet<Vertex> v, DataSet<Edge> e) {
    //Creating the map for the graphheads appearing in the logical graph
    this.graphHeadToVertex = v
      .flatMap(new AssociateElementToIdAndGraph<>())
      .joinWithTiny(this.graphHeads)
      .where(new Value0Of2<>()).equalTo(new SelfId())
      .with(new LeftSide<>())
      .filter(new ExceptGraphHead());

    //Same as above
    this.graphHeadToEdge = e
      .flatMap(new AssociateElementToIdAndGraph<>())
      .joinWithTiny(this.graphHeads)
      .where(new Value0Of2<>()).equalTo(new SelfId())
      .with(new LeftSide<>())
      .filter(new ExceptGraphHead());
  }
}
