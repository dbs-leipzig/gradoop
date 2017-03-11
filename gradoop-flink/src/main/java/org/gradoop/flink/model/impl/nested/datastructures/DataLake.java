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
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.nested.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.operators.BinaryOp;
import org.gradoop.flink.model.impl.nested.operators.UnaryOp;
import org.gradoop.flink.model.impl.nested.datastructures.functions.ByLabels;
import org.gradoop.flink.model.impl.nested.datastructures.functions.SelfId;
import org.gradoop.flink.model.impl.nested.datastructures.functions.LeftSideIfNotNull;
import org.gradoop.flink.model.impl.nested.datastructures.functions.MapVertexAsGraphHead;

/**
 * A DataLake represents a LogicalGraph containing all the ground truth information concerning
 * the whole graph process. this DataLake is going to be updated throught the execution of
 * binary or unary operations.
 *
 * Such operations are only performed over ids, and ids are also returned, but the actual graph
 * contains all the updated informations that are not returned within the ids.
 */
public class DataLake  {

  /**
   * Logical Graph containing all the actual values. Represents the flattened network
   */
  private NormalizedGraph dataLake;

  /**
   * Given the datalake extracts only its ids for the computation. This acts as an distributed
   * indexing structure.
   */
  private IdGraphDatabase dataLakeIdDatabase;

  /**
   * Optional, the data lake's name.
   */
  private String name;

  /**
   * Private constructor used only for internal usages. Directly assignes the elements
   * @param dataLake    Actual data information
   * @param igdb        Actual nesting information
   */
  private DataLake(NormalizedGraph dataLake, IdGraphDatabase igdb) {
    this.dataLake = dataLake;
    this.dataLakeIdDatabase = igdb;
  }

  /**
   * Initializes the DataLake from a LogicalGraph
   * @param dataLake    Data source
   */
  public DataLake(LogicalGraph dataLake) {
    this.dataLake = new NormalizedGraph(dataLake);
    dataLakeIdDatabase = new IdGraphDatabase(dataLake);
  }

  /**
   * Initializes the DataLake from a GraphCollection
   * @param dataLake    Data source
   */
  public DataLake(GraphCollection dataLake) {
    this.dataLake = new NormalizedGraph(dataLake);
    dataLakeIdDatabase = new IdGraphDatabase(dataLake);
  }

  /**
   * Initializes the DataLake from a original normalized information
   * @param dataLake    Data source.
   */
  public DataLake(NormalizedGraph dataLake) {
    this.dataLake = dataLake;
    dataLakeIdDatabase = new IdGraphDatabase(dataLake);
  }

  /**
   * Returns…
   * @return  the whole collection of edges appearing in the data lake
   */
  public DataSet<Edge> getEdges() {
    return dataLake.getEdges();
  }

  /**
   * Returns…
   * @return  the whole collection of vertices appearing in the data lake
   */
  public DataSet<Vertex> getVerteices() {
    return dataLake.getVertices();
  }

  /**
   * Returns…
   * @return  the id representation of the whole data lake
   */
  public IdGraphDatabase getIdDatabase() {
    return dataLakeIdDatabase;
  }

  /**
   * Returns…
   * @return  the actual representation containing all the values
   */
  public NormalizedGraph asNormalizedGraph() {
    return dataLake;
  }

  public DataLake extractGraphFromLabel(String label) {
    // the vertex could belong either to a former LogicalGraph (and hence, it appears as a vertex)
    // or as a vertex. The last case is always compliant, even for nested graphs.
    DataSet<GradoopId> extractHead = dataLake.getVertices()
      .filter(new ByLabel<>(label))
      .map(new Id<>());
    return extractGraphFromGradoopId(extractHead);
  }

  /**
   * Extracts a (sub) DataLake from the actual one,
   * @param labels      The graphs' unique labels that we want to extract for each subgraph
   * @return            A DataLake
   */
  public DataLake extractGraphFromLabel(String... labels) {
    // the vertex could belong either to a former LogicalGraph (and hence, it appears as a vertex)
    // or as a vertex. The last case is always compliant, even for nested graphs.
    DataSet<GradoopId> extractHead = dataLake.getVertices()
      .filter(new ByLabels<>(labels))
      .map(new Id<>());
    return extractGraphFromGradoopId(extractHead);
  }

  /**
   * Extracts a (sub) DataLake from the actual one,
   * @param gid         The graphs' graph id that is interesting for our operation
   * @return            A DataLake
   */
  public DataLake extractGraphFromGradoopId(GradoopId gid) {
    DataSet<GradoopId> toPass = dataLake
      .getConfig()
      .getExecutionEnvironment()
      .fromElements(gid);
    return extractGraphFromGradoopId(toPass);
  }

  /**
   * Extracts a (sub) DataLake from the actual one,
   * @param graphHead   The graphs' graph ids that are interesting for our operation
   * @return            A DataLake
   */
  public DataLake extractGraphFromGradoopId(DataSet<GradoopId> graphHead) {

    // Extracting the informations for the IdDatabase
    DataSet<Tuple2<GradoopId, GradoopId>> vertices = dataLakeIdDatabase
      .getGraphHeadToVertex()
      .joinWithTiny(graphHead)
      .where(new Value0Of2<>()).equalTo(new SelfId())
      .with(new LeftSide<>());
    DataSet<Tuple2<GradoopId, GradoopId>> edges = dataLakeIdDatabase
      .getGraphHeadToEdge()
      .joinWithTiny(graphHead)
      .where(new Value0Of2<>()).equalTo(new SelfId())
      .with(new LeftSide<>());

    /// Extracting the information
    DataSet<Vertex> subDataLakeVertices = dataLake.getVertices()
      .joinWithTiny(vertices)
      .where(new Id<>()).equalTo(new Value1Of2<>())
      .with(new LeftSide<>());

    DataSet<Edge> subDataLakeEdges = dataLake.getEdges()
      .joinWithTiny(edges)
      .where(new Id<>()).equalTo(new Value1Of2<>())
      .with(new LeftSide<>());

    // Checking if the id is a graph head.
    DataSet<GraphHead> actualGraphHead = dataLake.getGraphHeads()
      .joinWithTiny(graphHead)
      .where(new Id<>()).equalTo(new SelfId())
      .with(new LeftSide<>());

    DataSet<GraphHead> recreatedGraphHead = graphHead
      .joinWithHuge(dataLake.getVertices())
      .where(new SelfId()).equalTo(new Id<>())
      .with(new RightSide<>())
      .map(new MapVertexAsGraphHead());

    DataSet<GraphHead> toReturnGraphHead =
      // The recreated vertex is always there. It is not said for the
      actualGraphHead.rightOuterJoin(recreatedGraphHead)
      .where((GraphHead x)->0).equalTo((GraphHead x)->0)
      .with(new LeftSideIfNotNull<GraphHead>());

    NormalizedGraph lg = new NormalizedGraph(toReturnGraphHead,
                                                subDataLakeVertices,
                                                subDataLakeEdges,
                                                dataLake.getConfig());

    IdGraphDatabase igdb = new IdGraphDatabase(graphHead,vertices,edges);

    return new DataLake(lg,igdb);
  }

  /**
   * Performs a binary operation over the current graph datastructure, while updating it
   * @param op    Operation to be carried out
   * @param <X>   Acutal operand's instance
   * @return      The same operand, that now will only take the
   */
  public <X extends BinaryOp> X run(X op) {
    return op.setDataLake(this);
  }

  /**
   * Performs an unary operation over the current graph datastructure, while updating it
   * @param op    Operation to be carried out
   * @param <X>   Acutal operand's instance
   * @return      The same operand, that now will only take the
   */
  public <X extends UnaryOp> X run(X op) {
    return op.setDataLake(this);
  }

  /**
   * Update the edges' information by adding some other new
   * @param edges           New edges' values
   * @param tuple2DataSet   The nesting information for the edges in the first argument
   */
  public void incrementalUpdateEdges(DataSet<Edge> edges, DataSet<Tuple2<GradoopId, GradoopId>> tuple2DataSet) {
    dataLake.updateEdgesWithUnion(edges);
    dataLakeIdDatabase.addNewEdges(tuple2DataSet);
  }

  /**
   * Update the vertices' information by adding some other new
   * @param map     New vertices' values
   * @param cross   The nesting information for the vertices in the first argument
   */
  public void incrementalUpdateVertices(DataSet<Vertex> map,
    DataSet<Tuple2<GradoopId, GradoopId>> cross) {
    dataLake.updateVerticesWithUnion(map);
    dataLakeIdDatabase.addNewVertices(cross);
  }

  /**
   * Returns…
   * @return   the DataLake's optional name
   */
  public String getName() {
    return name;
  }

  /**
   * Optional: sets the DataLake's name
   * @param name  The actual name that has to be used
   */
  public void setName(String name) {
    this.name = name;
  }

}
