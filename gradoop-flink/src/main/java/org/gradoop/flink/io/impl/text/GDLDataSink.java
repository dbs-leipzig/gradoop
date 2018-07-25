package org.gradoop.flink.io.impl.text;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.text.functions.GraphTransactionToGDL;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.io.IOException;

public class GDLDataSink implements DataSink {

  private String path;

  public GDLDataSink(String path) {
    this.path = path;
  }

  @Override
  public void write(LogicalGraph logicalGraph) throws IOException {
    write(logicalGraph, false);
  }

  @Override
  public void write(GraphCollection graphCollection) throws
    IOException {
    write(graphCollection, false);
  }

  @Override
  public void write(LogicalGraph graph, boolean overwrite) throws IOException {
    write(graph.getConfig().getGraphCollectionFactory().fromGraph(graph), overwrite);
  }
  @Override
  public void write(GraphCollection graphCollection, boolean overWrite) throws IOException {
    FileSystem.WriteMode writeMode =
      overWrite ? FileSystem.WriteMode.OVERWRITE :  FileSystem.WriteMode.NO_OVERWRITE;

    graphCollection
      .getGraphTransactions()
      .map(new GraphTransactionToGDL())
      .output(new TextOutputFormat<>(new Path(path)))
      .setParallelism(1);
  }
}
