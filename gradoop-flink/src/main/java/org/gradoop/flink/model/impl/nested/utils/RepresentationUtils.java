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

package org.gradoop.flink.model.impl.nested.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.bool.Equals;
import org.gradoop.flink.model.impl.functions.epgm.GraphElementExpander;
import org.gradoop.flink.model.impl.functions.epgm.GraphVerticesEdges;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TransactionFromSets;
import org.gradoop.flink.model.impl.functions.utils.Cast;
import org.gradoop.flink.model.impl.nested.datastructures.NormalizedGraph;
import org.gradoop.flink.model.impl.nested.datastructures.equality
  .CanonicalAdjacencyMatrixBuilderForNormalizedGraphs;
import org.gradoop.flink.model.impl.nested.datastructures.functions.MapGraphHeadAsVertex;
import org.gradoop.flink.model.impl.nested.utils.functions.Tuple2Comparator;
import org.gradoop.flink.model.impl.nested.utils.functions.TupleOfIdsToString;
import org.gradoop.flink.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility Functions
 */
public class RepresentationUtils {

  /**
   * Writes a set of graph elements as a byte array, by using their GradoopId representation
   * @param set   Set of element to be written
   * @param <T>   Type of the graph element
   * @return      the byte array that has to be written
   * @throws IOException  If an error occurs…
   */
  public static <T extends GraphElement> byte[] toByteArray(Set<T> set) throws IOException {
    Set<byte[]> toTransform = new HashSet<>();
    for (T x : set) {
      toTransform.add(x.getId().toByteArray());
    }
    return privateToByteArray(toTransform);
  }

  /**
   * Writes a set of byte arrays as a byte array, by using their GradoopId representation
   * @param elements Set of arrays to be written
   * @return          written array
   */
  private static byte[] privateToByteArray(Set<byte[]> elements) throws IOException {
    try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
      try (ObjectOutputStream o = new ObjectOutputStream(b)) {
        o.writeObject(elements);
      }
      return b.toByteArray();
    }
  }

  /**
   * Convers an array of bytes into a set of bytes. bytes is just a default representation
   * @param bytes                   Array containing the object
   * @return                        Instantiated object
   */
  public static Set<byte[]> fromGraph(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream b = new ByteArrayInputStream(bytes)) {
      try (ObjectInputStream o = new ObjectInputStream(b)) {
        return (Set<byte[]>) o.readObject();
      }
    }
  }

  /**
   * Normalizes the current logical graph by adding the current graph head as a vertex.
   * @param lg    Logical graph to be extended (eventually) with a vertex
   * @return      The extended and mo
   */
  public static LogicalGraph normalize(LogicalGraph lg) {
    DataSet<Vertex> v = lg.getGraphHead()
      .map(new MapGraphHeadAsVertex())
      .union(lg.getVertices())
      .distinct(new Id<>());
    return LogicalGraph.fromDataSets(lg.getGraphHead(), v, lg.getEdges(), lg.getConfig());
  }

  /**
   * Normalizes the current graph collection as a LogicalGraph containing multiple graph heads
   * @param lg  collection of logical graphs to be flattened as a single collection of graphs
   * @return    The flattened version. A new vertex is created for each graph head
   */
  public static GraphCollection normalize(GraphCollection lg) {
    DataSet<Vertex> v = lg.getGraphHeads()
      .map(new MapGraphHeadAsVertex())
      .union(lg.getVertices())
      .distinct(new Id<>());
    return GraphCollection.fromDataSets(lg.getGraphHeads(), v, lg.getEdges(), lg.getConfig());
  }

  /**
   * Converts a GraphCollection to a GraphTransactons
   * @param gc    GraphCollection
   * @return      GraphTransaction
   */
  public static GraphTransactions toTransaction(GraphCollection gc) {
    return gc.toTransactions();
  }

  /**
   * Converts a LogicalGraph into a GraphTransaction
   * @param g   LogicalGraph
   * @return    GraphTransaction
   */
  public static GraphTransactions toTransaction(LogicalGraph g) {
    DataSet<Tuple2<GradoopId, GraphElement>> vertices = g.getVertices()
        .map(new Cast<>(GraphElement.class))
        .returns(TypeExtractor.getForClass(GraphElement.class))
        .flatMap(new GraphElementExpander<>());

    DataSet<Tuple2<GradoopId, GraphElement>> edges = g.getEdges()
        .map(new Cast<>(GraphElement.class))
        .returns(TypeExtractor.getForClass(GraphElement.class))
        .flatMap(new GraphElementExpander<>());

    DataSet<Tuple3<GradoopId, Set<Vertex>, Set<Edge>>> transactions = vertices
        .union(edges)
        .groupBy(0)
        .combineGroup(new GraphVerticesEdges())
        .groupBy(0)
        .reduceGroup(new GraphVerticesEdges());

    DataSet<GraphTransaction> graphTransactions = g.getGraphHead()
        .leftOuterJoin(transactions)
        .where(new Id<>()).equalTo(0)
        .with(new TransactionFromSets());

    return new GraphTransactions(graphTransactions, g.getConfig());
  }

  /**
   * Returns the canonical representation of a graph as a stirng
   * @param lg    Logical Graph
   * @return      Linearization
   */
  public static DataSet<String> utilCanonicalRepresentation(LogicalGraph lg) {
    return new CanonicalAdjacencyMatrixBuilder(new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(),
      true)
      .execute(GraphCollection.fromGraph(lg));
  }

  /**
   * Represents a NormalzedGraph as a string
   * @param lg    NormalizedGraph
   * @return      serialized representation
   */
  public static DataSet<String> utilCanonicalRepresentation(NormalizedGraph lg) {
    return new CanonicalAdjacencyMatrixBuilderForNormalizedGraphs(new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(),
      true)
      .execute(lg);
  }

  /**
   * Checks if two datasets are the same
   * @param left    Left element
   * @param right   Right element
   * @return        Result of the match
   */
  public static DataSet<Boolean> dataSetEquality(DataSet<Tuple2<GradoopId, GradoopId>> left,
    DataSet<Tuple2<GradoopId, GradoopId>> right) {
    return left.fullOuterJoin(right)
      .where(new TupleOfIdsToString()).equalTo(new TupleOfIdsToString())
      .with(new Tuple2Comparator());
  }

  /**
   * Checks if two datasets are the same
   * @param left    Left element
   * @param right   Right element
   * @return        Result of the match
   */
  public static DataSet<Boolean> dataSetEqualityIds(DataSet<GradoopId> left,
    DataSet<GradoopId> right) {
    return Equals.cross(left, right);
  }

}
