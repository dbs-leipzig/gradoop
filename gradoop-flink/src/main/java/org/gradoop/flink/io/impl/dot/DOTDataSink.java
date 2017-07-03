/**
 * Copyright © 2014 Gradoop (University of Leipzig - Database Research Group)
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

import org.apache.flink.core.fs.FileSystem;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.dot.functions.DOTFileFormat;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;

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
  public void write(GraphTransactions graphTransactions) throws
    IOException {

    write(graphTransactions, false);
  }


  @Override
  public void write(LogicalGraph logicalGraph, boolean overWrite) throws IOException {
    write(GraphCollection.fromGraph(logicalGraph).toTransactions(), overWrite);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overWrite) throws IOException {
    write(graphCollection.toTransactions(), overWrite);
  }

  @Override
  public void write(GraphTransactions graphTransactions, boolean overWrite) throws IOException {

    FileSystem.WriteMode writeMode =
      overWrite ? FileSystem.WriteMode.OVERWRITE :  FileSystem.WriteMode.NO_OVERWRITE;

    graphTransactions.getTransactions()
      .writeAsFormattedText(path, writeMode, new DOTFileFormat(graphInformation));
  }
}
