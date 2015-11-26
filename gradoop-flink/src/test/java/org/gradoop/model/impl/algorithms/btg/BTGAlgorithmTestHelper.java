//package org.gradoop.model.impl.algorithms.btg;
//
//import com.google.common.collect.Lists;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.graph.Edge;
//import org.apache.flink.graph.Graph;
//import org.apache.flink.graph.Vertex;
//import org.apache.flink.types.NullValue;
//import org.gradoop.model.impl.algorithms.btg.utils.BTGVertexType;
//import org.gradoop.model.impl.algorithms.btg.pojos.BTGVertexValue;
//import org.gradoop.model.impl.id.GradoopId;
//import org.gradoop.model.impl.id.GradoopIds;
//import org.gradoop.model.impl.pojo.VertexPojo;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.regex.Pattern;
//
//public class BTGAlgorithmTestHelper {
//  /**
//   * Used for splitting the line into the main tokens (vertex id, vertex value,
//   * edges)
//   */
//  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile("[,]");
//  /**
//   * Used for splitting a main token into its values (vertex value = type,
//   * value, btg-ids; edge list)
//   */
//  private static final Pattern VALUE_TOKEN_SEPARATOR = Pattern.compile("[ ]");
//
//  /**
//   * Function to parse a string array into a gelly graph for the
//   * FlinkBTGComputation
//   *
//   * @param graph String lines decodes vertices
//   * @param env   actual ExecutionEnvironment
//   * @return Gelly Graph based for FlinkBTGComputation
//   */
//  public static Graph<GradoopId, BTGVertexValue, NullValue> getGraph(String[] graph,
//    ExecutionEnvironment env) {
//    List<Vertex<GradoopId, BTGVertexValue>> vertices = new ArrayList<>();
//    List<Edge<GradoopId, NullValue>> edges = new ArrayList<>();
//    for (String line : graph) {
//      String[] lineTokens = LINE_TOKEN_SEPARATOR.split(line);
//      GradoopId id = GradoopIds.fromLongString(lineTokens[0]);
//      String[] valueTokens = VALUE_TOKEN_SEPARATOR.split(lineTokens[1]);
//      BTGVertexType vertexClass =
//        BTGVertexType.values()[Integer.parseInt(valueTokens[0])];
//      Double vertexValue = Double.parseDouble(valueTokens[1]);
//      List<GradoopId> btgIDs =
//        Lists.newArrayListWithCapacity(valueTokens.length - 1);
//      for (int n = 2; n < valueTokens.length; n++) {
//        btgIDs.add(GradoopIds.fromLongString(valueTokens[n]));
//      }
//      vertices.add(
//        new Vertex<>(id, new BTGVertexValue(vertexClass, vertexValue, btgIDs)));
//      String[] edgeTokens =
//        (lineTokens.length == 3) ? VALUE_TOKEN_SEPARATOR.split(lineTokens[2]) :
//          new String[0];
//      for (String edgeToken : edgeTokens) {
//        GradoopId tar = GradoopIds.fromLongString(edgeToken);
//        edges.add(new Edge<>(id, tar, NullValue.getInstance()));
//      }
//    }
//    return Graph.fromCollection(vertices, edges, env);
//  }
//
//  public static Map<GradoopId, List<GradoopId>> parseResultBTGVertices(
//    List<Vertex<GradoopId, BTGVertexValue>> vertices) {
//    Map<GradoopId, List<GradoopId>> result = new HashMap<>();
//    for (Vertex<GradoopId, BTGVertexValue> v : vertices) {
//      result.put(v.getId(), Lists.newArrayList(v.getValue().getGraphs()));
//    }
//    return result;
//  }
//
//  public static Map<GradoopId, List<GradoopId>> parseResultVertexPojos(
//    List<Vertex<GradoopId, VertexPojo>> graph) {
//    Map<GradoopId, List<GradoopId>> result = new HashMap<>();
//    for (Vertex<GradoopId, VertexPojo> v : graph) {
//      result.put(v.getId(), Lists.newArrayList(v.getValue().getGraphIds()));
//    }
//    return result;
//  }
//}
//
