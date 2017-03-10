package org.gradoop.flink.model.impl.nested.utils;

import org.apache.flink.api.common.functions.JoinFunction;
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
import org.gradoop.flink.model.impl.functions.epgm.GraphElementExpander;
import org.gradoop.flink.model.impl.functions.epgm.GraphVerticesEdges;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TransactionFromSets;
import org.gradoop.flink.model.impl.functions.utils.Cast;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;
import org.gradoop.flink.model.impl.nested.datastructures.NormalizedGraph;
import org.gradoop.flink.model.impl.nested.datastructures.equality
  .CanonicalAdjacencyMatrixBuilderForNormalizedGraphs;
import org.gradoop.flink.model.impl.nested.functions.AsQuadsMatchingSource;
import org.gradoop.flink.model.impl.nested.functions.Tuple2Comparator;
import org.gradoop.flink.model.impl.nested.functions.TupleOfIdsToString;
import org.gradoop.flink.model.impl.nested.tuples.Quad;
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
import java.util.stream.Collectors;

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
   * @throws IOException
   */
  private static byte[] privateToByteArray(Set<byte[]> elements) throws IOException {
    try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
      try(ObjectOutputStream o = new ObjectOutputStream(b)){
        o.writeObject(elements);
      }
      return b.toByteArray();
    }
  }

  public static Set<byte[]> fromGraph(byte[] bytes) throws IOException, ClassNotFoundException {
    try(ByteArrayInputStream b = new ByteArrayInputStream(bytes)){
      try(ObjectInputStream o = new ObjectInputStream(b)){
        return (Set<byte[]>)o.readObject();
      }
    }
  }

  public static DataSet<Quad> datalakeEdgesToQuadMatchingSource(DataLake dataLake) {
    return dataLake.getEdges().map(new AsQuadsMatchingSource());
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
    return LogicalGraph.fromDataSets(lg.getGraphHead(),v,lg.getEdges(),lg.getConfig());
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
    return GraphCollection.fromDataSets(lg.getGraphHeads(),v,lg.getEdges(),lg.getConfig());
  }

  public static GraphTransactions toTransaction(GraphCollection gc) {
    return gc.toTransactions();
  }

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

  public static String utilCanonicalRepresentation(LogicalGraph lg) {
    try {
      return new CanonicalAdjacencyMatrixBuilder(new GraphHeadToDataString(),
                                                  new VertexToDataString(),
                                                  new EdgeToDataString(),
                                                  true)
        .execute(GraphCollection.fromGraph(lg))
        .collect()
        .stream()
        .collect(Collectors.joining(" ++ "));
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }


  public static String utilCanonicalRepresentation(NormalizedGraph lg) {
    try {
      return new CanonicalAdjacencyMatrixBuilderForNormalizedGraphs(new GraphHeadToDataString(),
        new VertexToDataString(),
        new EdgeToDataString(),
        true)
        .execute(lg)
        .collect()
        .stream()
        .collect(Collectors.joining(" ++ "));
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public static DataSet<Boolean> dataSetEquality(DataSet<Tuple2<GradoopId, GradoopId>> left,
    DataSet<Tuple2<GradoopId, GradoopId>> right) {
    return left.fullOuterJoin(right)
      .where(new TupleOfIdsToString())
      .equalTo(new TupleOfIdsToString())
      .with(new Tuple2Comparator());
  }

}
