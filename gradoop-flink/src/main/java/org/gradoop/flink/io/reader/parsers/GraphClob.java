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

package org.gradoop.flink.io.reader.parsers;

import javafx.util.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.functions.InitEdge;
import org.gradoop.flink.io.impl.graph.functions.InitVertex;
import org.gradoop.flink.io.impl.graph.functions.UpdateEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.reader.parsers.rawedges.functions.CreateEdgesFromVertices;
import org.gradoop.flink.io.reader.parsers.rawedges.functions.CreateIdGraphDatabaseVertices;
import org.gradoop.flink.io.reader.parsers.rawedges.functions.MapRightEdgeToItsId;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Project3To0And1;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of3;
import org.gradoop.flink.model.impl.functions.tuple.Value2Of3;
import org.gradoop.flink.model.impl.nested.datastructures.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.datastructures.functions.SelfId;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;

/**
 * Represents a to-be-created graph as a DatSet of vertices and edges.
 *
 * @param <Element> comparable element
 */
public class GraphClob<Element extends Comparable<Element>> {

  /**
   * Representation of the parsed vertices
   */
  private DataSet<ImportVertex<Element>> vertices;

  /**
   * Representation of the parsed edges
   */
  private DataSet<ImportEdge<Element>> edges;

  /**
   * Default environment
   */
  private GradoopFlinkConfig env;

  /**
   * External id type
   */
  private TypeInformation<Element> externalIdType;

  /**
   * Adds the first chunk of the parsed graph within the data structure
   * @param vertices    Vertex Set
   * @param edges       Edge Set
   */
  public GraphClob(DataSet<ImportVertex<Element>> vertices, DataSet<ImportEdge<Element>> edges) {
    this(vertices, edges,
         GradoopFlinkConfig.createConfig(ExecutionEnvironment.getExecutionEnvironment()));
  }

  /**
   * Adds the first chunk of the parsed graph within the data structure
   * @param vertices    Vertex Set
   * @param edges       Edge Set
   * @param env         Environment
   */
  public GraphClob(DataSet<ImportVertex<Element>> vertices, DataSet<ImportEdge<Element>> edges,
                   GradoopFlinkConfig env) {
    this.vertices = vertices;
    this.edges = edges;
    this.env = env;
    this.externalIdType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(0);
  }

  /**
   * Changes the default configuration into a new one
   * @param conf  Uses a user-defined configuration
   */
  public void setGradoopFlinkConfiguration(GradoopFlinkConfig conf) {
    this.env = conf;
  }

  /**
   * Updates the current ParsableGraphClob with another one.
   * Merges the two to-be created graphs together
   * @param x To-be created graph
   * @return  The update instance of the current object
   */
  public GraphClob addAll(GraphClob<Element> x) {
    vertices = vertices.union(x.vertices);
    edges = edges.union(x.edges);
    return this;
  }

  /**
   * Updates the current GraphClob with another one.
   * Merges the two to-be created graphs together
   * @param v Vertex Set
   * @param e Edge Set
   * @return  The update instance of the current object
   */
  public GraphClob addAll(DataSet<ImportVertex<Element>> v, DataSet<ImportEdge<Element>> e) {
    vertices = vertices.union(v);
    edges = edges.union(e);
    return this;
  }

  /**
   * Transforms an external graph into an EPGM database.
   * @return  EPGM Database
   */
  public GraphDataSource<Element> asGraphDataSource() {
    return new GraphDataSource<>(vertices, edges, env);
  }

  public DataSet<Tuple3<Element, GradoopId, Vertex>> associateVertexToId() {
    return vertices
      .map(new InitVertex<>(env.getVertexFactory(), null, externalIdType));
  }

  /**
   * Istantiates a Logical Graph from this graph representation.
   */
  public LogicalGraph mapEdges() {
    DataSet<Tuple3<Element, GradoopId, Vertex>> avid = associateVertexToId();
    DataSet<Vertex> epgmVertices = avid
      .map(new Value2Of3<Element, GradoopId, Vertex>());

    DataSet<Tuple2<Element, GradoopId>> vertexIdPair = avid
      .map(new Project3To0And1<Element, GradoopId, Vertex>());

    DataSet<Edge> epgmEdges = edges
      .join(vertexIdPair)
      .where(1).equalTo(0)
      .with(new InitEdge<>(
        env.getEdgeFactory(), null, externalIdType))
      .join(vertexIdPair)
      .where(0).equalTo(0)
      .with(new UpdateEdge<>());


    GraphHead heads = env.getGraphHeadFactory().createGraphHead();
    DataSet<GraphHead> epgmHeads = env.getExecutionEnvironment().fromElements(heads);
    return LogicalGraph.fromDataSets(epgmHeads,epgmVertices,epgmEdges,env);
  }

  /**
   * Creates an IdGraphDatabase from a set of elements that are provided, so that it could
   * be used as an operator.
   * @param groups   A dataset of groups from which create full connected components
   * @return         A pair, where the first element is the IdGraphdatabase that could be
   *                 used as an operand. This part contains the representation of the list
   *                 of elements as a graph collection of many operands
   *
   *                 The second element is a dataset of edges, containing all the newly
   *                 created edges required.
   */
  public Pair<IdGraphDatabase, DataSet<Edge>> generateCollateralDataset(DataSet<List<Element>> groups) {
    // Raw vertices that could be written as plain elements
    DataSet<Tuple2<GradoopId,GradoopId>> idVertices = groups
      .flatMap(new ExtendListOfElementWithId<>(env.getGraphHeadFactory()))
      .join(associateVertexToId())
      .where(new Value1Of2<>()).equalTo(new Value1Of3<>())
      .with(new CreateIdGraphDatabaseVertices<>());

    // Edges that are created
    DataSet<Tuple2<GradoopId, Edge>> edges = idVertices
      .groupBy(0)
      .combineGroup(new CreateEdgesFromVertices(env.getEdgeFactory()));

    DataSet<Edge> epgmEdges = edges.map(new Value1Of2<>());

    // Final elements required to creat the id for the edges
    DataSet<Tuple2<GradoopId,GradoopId>> idEdges = edges.map(new MapRightEdgeToItsId());
    DataSet<GradoopId> idHead = idVertices.map(new Value0Of2<>()).distinct(new SelfId());

    // Id Graph Database
    IdGraphDatabase igdb = new IdGraphDatabase(idHead,idVertices,idEdges);
    return new Pair<>(igdb,epgmEdges);
  }

}
