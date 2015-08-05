package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.model.impl.operators.EPGLabelPropagationAlgorithm;
import org.gradoop.model.impl.operators.LabelPropagationValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Helper Class for LPTest
 */
public class LabelPropagationAlgorithmTestHelper {
  private static final Pattern SEPARATOR = Pattern.compile(" ");

  public static Graph<Long, LabelPropagationValue, NullValue> getGraph(
    String[] graph, ExecutionEnvironment env) {
    List<Vertex<Long, LabelPropagationValue>> vertices =
      Lists.newArrayList();
    List<Edge<Long, NullValue>> edges = Lists.newArrayList();
    for (String line : graph) {
      String[] tokens = SEPARATOR.split(line);
      long id = Long.parseLong(tokens[0]);
      long value = Long.parseLong(tokens[1]);
      vertices.add(new Vertex<>(id, new LabelPropagationValue(id, value)));
      for (int n = 2; n < tokens.length; n++) {
        long tar = Long.parseLong(tokens[n]);
        edges.add(new Edge<>(id, tar, NullValue.getInstance()));
      }
    }
    return Graph.fromCollection(vertices, edges, env);
  }


  public static Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> getEPGraph(
    String[] graph, ExecutionEnvironment env) {
    List<Vertex<Long, EPFlinkVertexData>> vertices =
      Lists.newArrayList();
    List<Edge<Long, EPFlinkEdgeData>> edges = Lists.newArrayList();
    for (String line : graph) {
      String[] tokens = SEPARATOR.split(line);
      long id = Long.parseLong(tokens[0]);
      long value = Long.parseLong(tokens[1]);
      vertices.add(new Vertex<>(id, getEPFlinkVertexValue(id, value)));
      for (int n = 2; n < tokens.length; n++) {
        long tar = Long.parseLong(tokens[n]);
        edges.add(new Edge<>(id, tar, getEPFlinkEdgeValue(id, tar)));
      }
    }
    return Graph.fromCollection(vertices, edges, env);
  }

  private static EPFlinkVertexData getEPFlinkVertexValue(long id, long value){
    Map<String, Object> props = new HashMap<>();
    props.put(EPGLabelPropagationAlgorithm.CURRENT_VALUE, value);
    props.put(EPGLabelPropagationAlgorithm.LAST_VALUE, Long.MAX_VALUE);
    props.put(EPGLabelPropagationAlgorithm.STABILIZATION_COUNTER, 0);
    props.put(EPGLabelPropagationAlgorithm.STABILIZATION_MAX, 20);
    return new EPFlinkVertexData(id, " ", props);
  }

  private static EPFlinkEdgeData getEPFlinkEdgeValue(long id, long tar){
    return new EPFlinkEdgeData(id, " ", id, tar);
  }
}
