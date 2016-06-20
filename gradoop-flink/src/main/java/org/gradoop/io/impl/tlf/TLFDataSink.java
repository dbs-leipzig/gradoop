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

package org.gradoop.io.impl.tlf;

import org.gradoop.io.api.DataSink;
import org.gradoop.io.impl.tlf.functions.GraphTransactionsToTLFFile;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Writes an EPGM representation into one TLF file. The format
 * is documented at {@link GraphTransactionsToTLFFile}.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class TLFDataSink
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends TLFBase<G, V, E>
  implements DataSink<G, V, E> {

  /**
   * Creates a new data sink. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param tlfPath tlf data file
   * @param config Gradoop Flink configuration
   */
  public TLFDataSink(String tlfPath, GradoopFlinkConfig<G, V, E>
    config) {
    super(tlfPath, "", "", config);
  }

  /**
   * Creates a new data sink. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param tlfPath tlf data file
   * @param tlfVertexDictionaryPath tlf vertex dictionary file
   * @param tlfEdgeDictionaryPath tlf edge dictionary file
   * @param config Gradoop Flink configuration
   */
  public TLFDataSink(String tlfPath, String tlfVertexDictionaryPath, String
    tlfEdgeDictionaryPath, GradoopFlinkConfig<G, V, E> config) {
    super(tlfPath, tlfVertexDictionaryPath, tlfEdgeDictionaryPath, config);
  }

  @Override
  public void write(LogicalGraph<G, V, E> logicalGraph) {
    write(GraphCollection.fromGraph(logicalGraph).toTransactions());
  }

  @Override
  public void write(GraphCollection<G, V, E> graphCollection) {
    write(graphCollection.toTransactions());
  }

  @Override
  public void write(GraphTransactions<G, V, E> graphTransactions) {
    graphTransactions.getTransactions().writeAsFormattedText(getTLFPath
      (), new GraphTransactionsToTLFFile<G, V, E>());
  }
}
