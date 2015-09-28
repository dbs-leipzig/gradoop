package org.gradoop.model.impl.labelpropagation;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.model.impl.DefaultEdgeData;
import org.gradoop.model.impl.DefaultEdgeDataFactory;
import org.gradoop.model.impl.DefaultVertexData;
import org.gradoop.model.impl.DefaultVertexDataFactory;
import org.gradoop.model.impl.operators.labelpropagation.EPGMLabelPropagationAlgorithm;
import org.gradoop.model.impl.operators.labelpropagation.LabelPropagationValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Helper Class for LPTest
 */
public class LabelPropagationTestHelper {
  private static final Pattern SEPARATOR = Pattern.compile(" ");

  public static Graph<Long, LabelPropagationValue, NullValue> getGraph(
    String[] graph, ExecutionEnvironment env) {
    List<Vertex<Long, LabelPropagationValue>> vertices = Lists.newArrayList();
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


  public static Graph<Long, DefaultVertexData, DefaultEdgeData> getEPGraph(
    String[] graph, ExecutionEnvironment env) {
    List<Vertex<Long, DefaultVertexData>> vertices = Lists.newArrayList();
    List<Edge<Long, DefaultEdgeData>> edges = Lists.newArrayList();
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

  private static DefaultVertexData getEPFlinkVertexValue(long id, long value) {
    Map<String, Object> props = new HashMap<>();
    props.put(EPGMLabelPropagationAlgorithm.CURRENT_VALUE, value);
    props.put(EPGMLabelPropagationAlgorithm.LAST_VALUE, Long.MAX_VALUE);
    props.put(EPGMLabelPropagationAlgorithm.STABILIZATION_COUNTER, 0);
    props.put(EPGMLabelPropagationAlgorithm.STABILIZATION_MAX, 20);
    return new DefaultVertexDataFactory().createVertexData(id, " ", props);
  }

  private static DefaultEdgeData getEPFlinkEdgeValue(long id, long tar) {
    return new DefaultEdgeDataFactory().createEdgeData(id, " ", id, tar);
  }
}
