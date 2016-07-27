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

package org.gradoop.io.impl.dot;

import org.gradoop.io.api.DataSink;
import org.gradoop.io.impl.dot.functions.DotFileFormat;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;

import java.io.IOException;

/**
 * Writes an EPGM representation into one DOT file. The format
 * is documented at {@link DotFileFormat}.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class DotDataSink
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements DataSink<G, V, E> {
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
  public DotDataSink(String path, boolean graphInformation) {
    this.path = path;
    this.graphInformation = graphInformation;
  }


  @Override
  public void write(LogicalGraph<G, V, E> logicalGraph) throws IOException {
    write(GraphCollection.fromGraph(logicalGraph).toTransactions());
  }

  @Override
  public void write(GraphCollection<G, V, E> graphCollection) throws
    IOException {
    write(graphCollection.toTransactions());
  }

  @Override
  public void write(GraphTransactions<G, V, E> graphTransactions) throws
    IOException {
    graphTransactions.getTransactions()
      .writeAsFormattedText(path, new DotFileFormat<G, V, E>(graphInformation));
  }
}
