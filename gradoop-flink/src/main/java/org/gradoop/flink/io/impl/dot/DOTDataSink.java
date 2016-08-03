/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.dot;

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
    write(GraphCollection.fromGraph(logicalGraph).toTransactions());
  }

  @Override
  public void write(GraphCollection graphCollection) throws
    IOException {
    write(graphCollection.toTransactions());
  }

  @Override
  public void write(GraphTransactions graphTransactions) throws
    IOException {
    graphTransactions.getTransactions()
      .writeAsFormattedText(path, new DOTFileFormat(graphInformation));
  }
}
