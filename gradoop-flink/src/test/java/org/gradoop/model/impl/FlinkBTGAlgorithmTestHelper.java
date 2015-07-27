package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.model.impl.operators.io.formats.FlinkBTGVertexType;
import org.gradoop.model.impl.operators.io.formats.FlinkBTGVertexValue;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class FlinkBTGAlgorithmTestHelper {
  /**
   * Used for splitting the line into the main tokens (vertex id, vertex value,
   * edges)
   */
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile("[,]");
  /**
   * Used for splitting a main token into its values (vertex value = type,
   * value, btg-ids; edge list)
   */
  private static final Pattern VALUE_TOKEN_SEPARATOR = Pattern.compile("[ ]");

  private static String[] getConnectedIIG() {
    return new String[] {
      "0,1 0,1 4 9 10", "1,1 1,0 5 6 11 12", "2,1 2,8 13", "3,1 3,7 14 15",
      "4,0 4,0 5", "5,0 5,1 4 6", "6,0 6,1 5 7 8", "7,0 7,3 6", "8,0 8,2 6",
      "9,0 9,0 10", "10,0 10,0 9 11 12", "11,0 11,1 10 13 14",
      "12,0 12,1 10 15", "13,0 13,2 11", "14,0 14,3 11", "15,0 15,3 12"
    };
  }

  public static DataSet<Vertex<Long, FlinkBTGVertexValue>>
  getConnectedIIGVertices(
    ExecutionEnvironment env) {
    List<Vertex<Long, FlinkBTGVertexValue>> vertices = new ArrayList<>();
    String[] graph = getConnectedIIG();
    for (String line : graph) {
      String[] lineTokens = LINE_TOKEN_SEPARATOR.split(line);
      long id = Long.parseLong(lineTokens[0]);
      String[] valueTokens = VALUE_TOKEN_SEPARATOR.split(lineTokens[1]);
      FlinkBTGVertexType vertexClass =
        FlinkBTGVertexType.values()[Integer.parseInt(valueTokens[0])];
      Double vertexValue = Double.parseDouble(valueTokens[1]);
      List<Long> btgIDs =
        Lists.newArrayListWithCapacity(valueTokens.length - 1);
      for (int n = 2; n < valueTokens.length; n++) {
        btgIDs.add(Long.parseLong(valueTokens[n]));
      }
      vertices.add(new Vertex<>(id,
        new FlinkBTGVertexValue(vertexClass, vertexValue, btgIDs)));
    }
    return env.fromCollection(vertices);
  }

  public static DataSet<Edge<Long, Long>> getConnectedIIGEdges(
    ExecutionEnvironment env) {
    List<Edge<Long, Long>> edges = new ArrayList<>();
    String[] graph = getConnectedIIG();
    for (String line : graph) {
      String[] lineTokens = LINE_TOKEN_SEPARATOR.split(line);
      long id = Long.parseLong(lineTokens[0]);
      String[] edgeTokens =
        (lineTokens.length == 3) ? VALUE_TOKEN_SEPARATOR.split(lineTokens[2]) :
          new String[0];
      for (String edgeToken : edgeTokens) {
        long tar = Long.parseLong(edgeToken);
        edges.add(new Edge<Long, Long>(id, tar, 0L));
      }
    }
    return env.fromCollection(edges);
  }
}

