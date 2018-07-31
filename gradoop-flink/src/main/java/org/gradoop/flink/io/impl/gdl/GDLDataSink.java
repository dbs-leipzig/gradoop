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
package org.gradoop.flink.io.impl.gdl;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.gdl.functions.GraphTransactionsToGDL;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.io.IOException;

/**
 * A data sink that writes a graph or graph collection to a gdl formatted string.
 * It is executed with a parallelism of 1, and therefore limited to smaller graphs.
 */
public class GDLDataSink implements DataSink {

  /**
   * The path to write the file to.
   */
  private String path;

  /**
   * Creates a GDL data sink.
   *
   * @param path The path to write the file to.
   */
  public GDLDataSink(String path) {
    this.path = path;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(LogicalGraph logicalGraph) throws IOException {
    write(logicalGraph, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(GraphCollection graphCollection) throws IOException {
    write(graphCollection, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(LogicalGraph logicalGraph, boolean overwrite) throws IOException {
    write(logicalGraph.getConfig().getGraphCollectionFactory().fromGraph(logicalGraph), overwrite);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(GraphCollection graphCollection, boolean overwrite) throws IOException {
    FileSystem.WriteMode writeMode =
      overwrite ? FileSystem.WriteMode.OVERWRITE :  FileSystem.WriteMode.NO_OVERWRITE;

    TextOutputFormat<String> textOutputFormat = new TextOutputFormat<>(new Path(path));
    textOutputFormat.setWriteMode(writeMode);

    graphCollection
      .getGraphTransactions()
      .reduceGroup(new GraphTransactionsToGDL())
      .output(textOutputFormat)
      .setParallelism(1);
  }
}
