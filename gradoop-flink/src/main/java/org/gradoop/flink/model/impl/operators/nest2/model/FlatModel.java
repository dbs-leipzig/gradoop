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

package org.gradoop.flink.model.impl.operators.nest2.model;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.nest.transformations
  .EPGMToNestedIndexingTransformation;
import org.gradoop.flink.model.impl.operators.nest2.model.indices.NestedIndexing;

/**
 * A FlatModel represents a LogicalGraph containing all the ground truth information concerning
 * the whole graph process. this DataLake is going to be updated throught the execution of
 * binary or unary operations.
 *
 * Such operations are only performed over ids, and ids are also returned, but the actual graph
 * contains all the updated informations that are not returned within the ids.
 */
public class FlatModel {

  /**
   * Logical Graph containing all the actual values. Represents the flattened network
   */
  private NormalizedGraph dataLake;

  /**
   * Given the datalake extracts only its ids for the computation. This acts as an distributed
   * indexing structure.
   */
  private NestedIndexing dataLakeIdDatabase;

  /**
   * Optional, the data lake's name.
   */
  private String name;

  /**
   * Private constructor used only for internal usages. Directly assignes the elements
   * @param dataLake    Actual data information
   * @param igdb        Actual nesting information
   */
  public FlatModel(NormalizedGraph dataLake, NestedIndexing igdb) {
    this.dataLake = dataLake;
    this.dataLakeIdDatabase = igdb;
  }

  /**
   * Initializes the DataLake from a LogicalGraph
   * @param dataLake    Data source
   */
  public FlatModel(LogicalGraph dataLake) {
    this.dataLake = new NormalizedGraph(dataLake);
    dataLakeIdDatabase = EPGMToNestedIndexingTransformation.fromLogicalGraph(dataLake);
  }

  /**
   * Initializes the DataLake from a GraphCollection
   * @param dataLake    Data source
   */
  public FlatModel(GraphCollection dataLake) {
    this.dataLake = new NormalizedGraph(dataLake);
    dataLakeIdDatabase = EPGMToNestedIndexingTransformation.fromGraphCollection(dataLake);
  }

  /**
   * Initializes the DataLake from a original normalized information
   * @param dataLake    Data source.
   */
  public FlatModel(NormalizedGraph dataLake) {
    this.dataLake = dataLake;
    dataLakeIdDatabase = EPGMToNestedIndexingTransformation.fromNormalizedGraph(dataLake);
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
  public DataSet<Vertex> getVertices() {
    return dataLake.getVertices();
  }

  /**
   * Returns…
   * @return  the id representation of the whole data lake
   */
  public NestedIndexing getIdDatabase() {
    return dataLakeIdDatabase;
  }

  /**
   * Returns…
   * @return  the actual representation containing all the values
   */
  public NormalizedGraph asNormalizedGraph() {
    return dataLake;
  }

  public DataSet<GradoopId> generateNewSingleton(GradoopId element) {
    return dataLake.getConfig().getExecutionEnvironment().fromElements(element);
  }

  /**
   * Update the edges' information by adding some other new
   * @param edges           New edges' values
   * @param tuple2DataSet   The nesting information for the edges in the first argument
   */
  public void incrementalUpdateEdges(DataSet<Edge> edges,
                                     DataSet<Tuple2<GradoopId, GradoopId>> tuple2DataSet) {
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
