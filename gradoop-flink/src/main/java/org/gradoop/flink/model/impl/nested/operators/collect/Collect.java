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
package org.gradoop.flink.model.impl.nested.operators.collect;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.datastructures.IdGraphDatabase;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * This class collects IdGraphDatabase into one single graph database.
 * Please note that this is not the union operation, while this is the
 * collection creation, where each component is defined as a
 */
public class Collect {

  /**
   * Header containing all the graphs within the collection
   */
  private DataSet<GradoopId> head;

  /**
   * Vertices contained by each graph within the collection
   */
  private DataSet<Tuple2<GradoopId,GradoopId>> vertices;

  /**
   * Edges contained by each graph within  the collection
   */
  private DataSet<Tuple2<GradoopId,GradoopId>> edges;

  /**
   * Initializes the operator by using the default configuration
   * @param conf
   */
  public Collect(GradoopFlinkConfig conf) {
    this.head = conf.getExecutionEnvironment().fromElements();
    this.vertices = conf.getExecutionEnvironment().fromElements();
    this.edges = conf.getExecutionEnvironment().fromElements();
  }

  /**
   * Adds a graph, represented by its ids, into the collection
   * @param x Graph to be added to the collection
   */
  public void add(IdGraphDatabase x) {
    head = head.union(x.getGraphHeads());
    vertices = vertices.union(x.getGraphHeadToVertex());
    edges = edges.union(x.getGraphHeadToEdge());
  }

  /**
   * Adds a collection of graphs to the collection, represented as one single graph
   * @param graphIds  Graphs to be added to the colletion
   */
  public void addAll(IdGraphDatabase... graphIds) {
    for (IdGraphDatabase x : graphIds) {
      add(x);
    }
  }

  /**
   * Instantiates the graph to a specific graph, that is a graph of collections
   * @return  The aforementioned result.
   */
  public IdGraphDatabase asIdGraphDatabase() {
    return new IdGraphDatabase(head,vertices,edges);
  }

}
