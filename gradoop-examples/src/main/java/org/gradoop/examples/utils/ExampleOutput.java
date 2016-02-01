package org.gradoop.examples.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.cam.CanonicalAdjacencyMatrix;
import org.gradoop.model.impl.operators.cam.functions.EdgeDataLabeler;
import org.gradoop.model.impl.operators.cam.functions.GraphHeadDataLabeler;
import org.gradoop.model.impl.operators.cam.functions.VertexDataLabeler;

import java.util.ArrayList;

public class ExampleOutput
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  private DataSet<ArrayList<String>> outSet;

  public void add(String caption, LogicalGraph<G, V, E> graph) {
    add(caption, GraphCollection.fromGraph(graph));
  }

  public void add(String caption, GraphCollection<G, V, E> collection) {

    if(outSet == null) {
      outSet = collection
        .getConfig().getExecutionEnvironment()
        .fromElements(new ArrayList<String>());
    }

    DataSet<String> captionSet =
      collection
        .getConfig().getExecutionEnvironment()
        .fromElements("\n*** " + caption + " ***\n");

    DataSet<String> graphStringSet =
      new CanonicalAdjacencyMatrix<>(
        new GraphHeadDataLabeler<G>(),
        new VertexDataLabeler<V>(),
        new EdgeDataLabeler<E>()
      ).execute(collection);

    outSet = outSet
      .cross(captionSet)
      .with(new OutputAppender())
      .cross(graphStringSet)
      .with(new OutputAppender());
  }

  public void print() throws Exception {
    outSet
      .map(new LineCombiner())
      .print();
  }

  private class OutputAppender
    implements CrossFunction<ArrayList<String>, String, ArrayList<String>> {

    @Override
    public ArrayList<String> cross(ArrayList<String> out, String line) throws
      Exception {

      out.add(line);

      return out;
    }
  }

  private class LineCombiner implements MapFunction<ArrayList<String>, String> {

    @Override
    public String map(ArrayList<String> lines) throws Exception {

      return StringUtils.join(lines, "\n");
    }
  }
}
