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
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;
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

  public static Graph<GradoopId, LPVertexValue, NullValue> getGraph(
    String[] graph, ExecutionEnvironment env) {
    List<Vertex<GradoopId, LPVertexValue>> vertices = Lists.newArrayList();
    List<Edge<GradoopId, NullValue>> edges = Lists.newArrayList();
    for (String line : graph) {
      String[] tokens = SEPARATOR.split(line);
      GradoopId id = GradoopIds.fromLongString(tokens[0]);
      GradoopId value = GradoopIds.fromLongString(tokens[1]);
      vertices.add(new Vertex<>(id, new LPVertexValue(id, value)));
      for (int n = 2; n < tokens.length; n++) {
        GradoopId tar = GradoopIds.fromLongString(tokens[n]);
        edges.add(new Edge<>(id, tar, NullValue.getInstance()));
      }
    }
    return Graph.fromCollection(vertices, edges, env);
  }


  public static Graph<GradoopId, VertexPojo, EdgePojo> getEPGraph(
    String[] graph, ExecutionEnvironment env) {
    List<Vertex<GradoopId, VertexPojo>> vertices = Lists.newArrayList();
    List<Edge<GradoopId, EdgePojo>> edges = Lists.newArrayList();
    for (String line : graph) {
      String[] tokens = SEPARATOR.split(line);
      GradoopId id = GradoopIds.fromLongString(tokens[0]);
      GradoopId value = GradoopIds.fromLongString(tokens[1]);
      vertices.add(new Vertex<>(id, getEPFlinkVertexValue(id, value)));
      for (int n = 2; n < tokens.length; n++) {
        GradoopId tar = GradoopIds.fromLongString(tokens[n]);
        edges.add(new Edge<>(id, tar, getEPFlinkEdgeValue(id, tar)));
      }
    }
    return Graph.fromCollection(vertices, edges, env);
  }

  private static VertexPojo getEPFlinkVertexValue(GradoopId id, GradoopId value) {
    Map<String, Object> props = new HashMap<>();
    props.put(EPGMLabelPropagationAlgorithm.CURRENT_VALUE, value);
    props.put(EPGMLabelPropagationAlgorithm.LAST_VALUE, GradoopIds.MAX_VALUE);
    props.put(EPGMLabelPropagationAlgorithm.STABILIZATION_COUNTER, 0);
    props.put(EPGMLabelPropagationAlgorithm.STABILIZATION_MAX, 20);
    return new VertexPojoFactory().createVertex(id, " ", props);
  }

  private static EdgePojo getEPFlinkEdgeValue(GradoopId id, GradoopId tar) {
    return new EdgePojoFactory().createEdge(id, " ", id, tar);
  }
}
