package org.gradoop.examples.sink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMEdgeFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMVertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Sink example.
 */
public class DotSinkExample {
  /**
   * Main
   * @param args arguments from cli
   */
  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    EPGMVertexFactory vertexFactory = new EPGMVertexFactory();
    EPGMEdgeFactory edgeFactory = new EPGMEdgeFactory();

    Properties properties = Properties.create();
    properties.set("title", "In the books Wojo's Weapons, why was Wojo occasionally known to say \"I'm too lazy!\" when asked why he didn't play 1. d4 instead of 1. Nf3?");

    EPGMVertex vertex = vertexFactory.initVertex(GradoopId.get(), "book", properties);
    EPGMEdge edge = edgeFactory.createEdge("identity", vertex.getId(), vertex.getId());

    List<EPGMVertex> vertices = new ArrayList<>();
    vertices.add(vertex);
    List<EPGMEdge> edges = new ArrayList<>();
    edges.add(edge);

    LogicalGraph graph = config.getLogicalGraphFactory().fromCollections(vertices, edges);

    DataSink sink = new DOTDataSink("example_out.dot", true, DOTDataSink.DotFormat.HTML);

    sink.write(graph);

    env.execute();

  }
}
