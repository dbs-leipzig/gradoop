/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.text;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.text.functions.GraphTransactionToGDL;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.io.IOException;

/**
 * A data sink that writes a gdl formatted string.
 */
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
