package org.gradoop.model.impl.operators;

import com.google.common.collect.Lists;
import com.sun.tools.javac.comp.Todo;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.model.impl.EPFlinkEdgeData;
import org.gradoop.model.impl.EPFlinkVertexData;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.impl.operators.io.formats.BTGVertexType;
import org.gradoop.model.impl.operators.io.formats.BTGVertexValue;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * blubb
 */
public class BTG implements UnaryGraphToCollectionOperator, Serializable {
  private int maxIterations;
  private final ExecutionEnvironment env;
  private static final String VERTEX_TYPE_PROPERTYKEY = "vertex.type";
  private static final String VERTEX_VALUE_PROPERTYKEY = "vertex.value";
  private static final String VERTEX_BTGIDS_PROPERTYKEY = "vertex.btgids";

  public BTG(int maxIterations, ExecutionEnvironment env) {
    this.maxIterations = maxIterations;
    this.env = env;
  }

  @Override
  public EPGraphCollection execute(EPGraph graph) {
    DataSet<Vertex<Long, BTGVertexValue>> vertices =
      graph.getGellyGraph().getVertices().map(
        new MapFunction<Vertex<Long, EPFlinkVertexData>, Vertex<Long,
          BTGVertexValue>>() {
          @Override
          public Vertex<Long, BTGVertexValue> map(
            Vertex<Long, EPFlinkVertexData> epVertex) throws Exception {
            return new Vertex<Long, BTGVertexValue>(epVertex.getId(),
              getBTGValue(epVertex));
          }
        });
    DataSet<Edge<Long, NullValue>> edges = graph.getGellyGraph().getEdges().map(
      new MapFunction<Edge<Long, EPFlinkEdgeData>, Edge<Long, NullValue>>() {
        @Override
        public Edge<Long, NullValue> map(
          Edge<Long, EPFlinkEdgeData> edge) throws Exception {
          return new Edge<Long, NullValue>(edge.getSource(), edge.getTarget(),
            NullValue.getInstance());
        }
      });
    Graph<Long, BTGVertexValue, NullValue> btgGraph =
      Graph.fromDataSet(vertices, edges, env);
    try {
      btgGraph = btgGraph.run(new BTGAlgorithm(this.maxIterations));
    } catch (Exception e) {
      e.printStackTrace();
    }
    DataSet<Vertex<Long, EPFlinkVertexData>> btgLabeledVertices =
      btgGraph.getVertices().join(graph.getGellyGraph().getVertices())
        .where(new BTGKeySelector()).equalTo(new VertexKeySelector())
        .with(new BTGJoin());
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> gellyBTGGraph = Graph
      .fromDataSet(btgLabeledVertices, graph.getGellyGraph().getEdges(), env);
    EPGraph btgEPGraph = EPGraph.fromGraph(gellyBTGGraph, null);
    //Todo: Create OverlapSplitBy Operator
    return null;
  }

  private static BTGVertexValue getBTGValue(
    Vertex<Long, EPFlinkVertexData> vertex) {
    BTGVertexType vertexType = BTGVertexType.values()[(int) vertex.getValue()
      .getProperty(VERTEX_TYPE_PROPERTYKEY)];
    double vertexValue =
      (double) vertex.getValue().getProperty(VERTEX_VALUE_PROPERTYKEY);
    ArrayList vertexBTGids = Lists.newArrayList(
      (List) vertex.getValue().getProperty(VERTEX_BTGIDS_PROPERTYKEY));
    return new BTGVertexValue(vertexType, vertexValue, vertexBTGids);
  }

  private static class BTGKeySelector implements
    KeySelector<Vertex<Long, BTGVertexValue>, Long> {
    @Override
    public Long getKey(Vertex<Long, BTGVertexValue> btgVertex) throws
      Exception {
      return btgVertex.getId();
    }
  }

  private static class VertexKeySelector implements
    KeySelector<Vertex<Long, EPFlinkVertexData>, Long> {
    @Override
    public Long getKey(Vertex<Long, EPFlinkVertexData> epVertex) throws
      Exception {
      return epVertex.getId();
    }
  }

  private static class BTGJoin implements
    JoinFunction<Vertex<Long, BTGVertexValue>, Vertex<Long,
      EPFlinkVertexData>, Vertex<Long, EPFlinkVertexData>> {
    @Override
    public Vertex<Long, EPFlinkVertexData> join(
      Vertex<Long, BTGVertexValue> btgVertex,
      Vertex<Long, EPFlinkVertexData> epVertex) throws Exception {
      epVertex.getValue().setProperty(VERTEX_TYPE_PROPERTYKEY,
        btgVertex.getValue().getVertexType());
      epVertex.getValue().setProperty(VERTEX_VALUE_PROPERTYKEY,
        btgVertex.getValue().getVertexValue());
      epVertex.getValue().setProperty(VERTEX_BTGIDS_PROPERTYKEY,
        btgVertex.getValue().getGraphs());
      return epVertex;
    }
  }

  @Override
  public String getName() {
    return null;
  }
}
