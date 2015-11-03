package org.gradoop.model.impl.algorithms.labelpropagation;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.model.impl.algorithms.epgmlabelpropagation
  .EPGMLabelPropagationAlgorithm;
import org.gradoop.model.impl.algorithms.labelpropagation.pojos.LPVertexValue;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.EdgePojoFactory;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.pojo.VertexPojoFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Helper Class for LPTest
 */
public class LabelPropagationTestHelper {
  private static final Pattern SEPARATOR = Pattern.compile(" ");

  public static Graph<Long, LPVertexValue, NullValue> getGraph(
    String[] graph, ExecutionEnvironment env) {
    List<Vertex<Long, LPVertexValue>> vertices = Lists.newArrayList();
    List<Edge<Long, NullValue>> edges = Lists.newArrayList();
    for (String line : graph) {
      String[] tokens = SEPARATOR.split(line);
      long id = Long.parseLong(tokens[0]);
      long value = Long.parseLong(tokens[1]);
      vertices.add(new Vertex<>(id, new LPVertexValue(id, value)));
      for (int n = 2; n < tokens.length; n++) {
        long tar = Long.parseLong(tokens[n]);
        edges.add(new Edge<>(id, tar, NullValue.getInstance()));
      }
    }
    return Graph.fromCollection(vertices, edges, env);
  }


  public static Graph<Long, VertexPojo, EdgePojo> getEPGraph(
    String[] graph, ExecutionEnvironment env) {
    List<Vertex<Long, VertexPojo>> vertices = Lists.newArrayList();
    List<Edge<Long, EdgePojo>> edges = Lists.newArrayList();
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

  private static VertexPojo getEPFlinkVertexValue(long id, long value) {
    Map<String, Object> props = new HashMap<>();
    props.put(EPGMLabelPropagationAlgorithm.CURRENT_VALUE, value);
    props.put(EPGMLabelPropagationAlgorithm.LAST_VALUE, Long.MAX_VALUE);
    props.put(EPGMLabelPropagationAlgorithm.STABILIZATION_COUNTER, 0);
    props.put(EPGMLabelPropagationAlgorithm.STABILIZATION_MAX, 20);
    return new VertexPojoFactory().createVertex(id, " ", props);
  }

  private static EdgePojo getEPFlinkEdgeValue(long id, long tar) {
    return new EdgePojoFactory().createEdge(id, " ", id, tar);
  }
}
