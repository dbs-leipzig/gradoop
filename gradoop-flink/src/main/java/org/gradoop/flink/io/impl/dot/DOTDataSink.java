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
package org.gradoop.flink.io.impl.dot;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.dot.functions.DOTFileFormat;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.io.IOException;

/**
 * Writes an EPGM representation into one DOT file. The format
 * is documented at {@link DOTFileFormat}.
 *
 * For more information see:
 * https://en.wikipedia.org/wiki/DOT_(graph_description_language)
 */
public class DOTDataSink implements DataSink {
  /**
   * Destination path of the dot file
   */
  private final String path;
  /**
   * Flag to print graph head information
   */
  private final boolean graphInformation;

  /**
   * Creates a new data sink. Path can be local (file://) or HDFS (hdfs://).
   *
   * @param path              dot data file
   * @param graphInformation  flag to print graph head information
   */
  public DOTDataSink(String path, boolean graphInformation) {
    this.path = path;
    this.graphInformation = graphInformation;
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
  public void write(GraphCollection graphCollection, boolean overwrite) throws IOException {
    FileSystem.WriteMode writeMode =
      overwrite ? FileSystem.WriteMode.OVERWRITE :  FileSystem.WriteMode.NO_OVERWRITE;

    DOTFileFormat dotFileFormat = new DOTFileFormat(graphInformation);
    GraphvizWriter graphvizWriter = new GraphvizWriter(new Path(path));
    graphvizWriter.setWriteMode(writeMode);

    graphCollection
      .getGraphTransactions()
      .map(tx -> dotFileFormat.format(tx))
      .output(graphvizWriter)
      .setParallelism(1);
  }

  /**
   * Write opening and closing lines around strings
   * representing individual {@link GraphTransaction}s in graphviz.
   */
  private static class GraphvizWriter extends TextOutputFormat<String> {

    /**
     * Default class version for serialization.
     */
    private static final long serialVersionUID = 1;

    /**
     * see super constructor.
     * @param outputPath graphviz dot file name
     * @param charset encoding
     */
    public GraphvizWriter(Path outputPath, String charset) {
      super(outputPath, charset);
    }

    /**
     * see super constructor.
     * @param outputPath graphviz dot file name
     */
    public GraphvizWriter(Path outputPath) {
      super(outputPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
      super.open(taskNumber, numTasks);
      super.writeRecord("digraph {\n");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
      super.writeRecord("}");
      super.close();
    }
  }
}
