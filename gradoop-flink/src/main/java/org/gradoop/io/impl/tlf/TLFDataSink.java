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

import org.apache.flink.api.java.DataSet;
import org.gradoop.io.api.DataSink;
import org.gradoop.io.impl.tlf.functions.EdgeLabelList;
import org.gradoop.io.impl.tlf.functions.TLFDictionaryFileFormat;
import org.gradoop.io.impl.tlf.functions.ElementLabelEncoder;
import org.gradoop.io.impl.tlf.functions.TLFFileFormat;
import org.gradoop.io.impl.tlf.constants.BroadcastNames;
import org.gradoop.io.impl.tlf.functions.VertexLabelList;
import org.gradoop.io.impl.tlf.functions.TLFDictionaryMapGroupReducer;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.Map;


/**
 * Writes an EPGM representation into one TLF file. The format
 * is documented at {@link TLFFileFormat}.
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
  public TLFDataSink(String tlfPath, GradoopFlinkConfig<G, V, E> config) {
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
  public TLFDataSink(String tlfPath, String tlfVertexDictionaryPath,
    String tlfEdgeDictionaryPath, GradoopFlinkConfig<G, V, E> config) {
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
    DataSet<GraphTransaction<G, V, E>> simpleLabelTransaction;
    DataSet<Map<String, Integer>> vertexDictionary = null;
    DataSet<Map<String, Integer>> edgeDictionary = null;
    // if the graph transaction vertex labels are set by a dictionary
    if (hasVertexDictionary()) {
      vertexDictionary = graphTransactions.getTransactions()
        // get a vertex dictionary for each transaction
        .flatMap(new VertexLabelList<G, V, E>())
        // reduce them to one dictionary without duplicates
        .reduceGroup(new TLFDictionaryMapGroupReducer());
      // write the vertex dictionary
      vertexDictionary
        .writeAsFormattedText(getTLFVertexDictionaryPath(),
          new TLFDictionaryFileFormat());
    }

    if (hasEdgeDictionary()) {
      edgeDictionary = graphTransactions.getTransactions()
        // get an edge dictionary for each transaction
        .flatMap(new EdgeLabelList<G, V, E>())
        // reduce them to one dictionary without duplicates
        .reduceGroup(new TLFDictionaryMapGroupReducer());
      // write the edge dictionary
      edgeDictionary
        .writeAsFormattedText(getTLFEdgeDictionaryPath(),
          new TLFDictionaryFileFormat());
    }


    if (hasVertexDictionary() || hasEdgeDictionary()) {
      if (hasVertexDictionary() && hasEdgeDictionary()) {
        simpleLabelTransaction = graphTransactions.getTransactions()
          // map the simple integer-like labels
          .map(new ElementLabelEncoder<G, V, E>(
            hasVertexDictionary(), hasEdgeDictionary()))
          .withBroadcastSet(vertexDictionary,
            BroadcastNames.VERTEX_DICTIONARY)
          .withBroadcastSet(edgeDictionary,
            BroadcastNames.EDGE_DICTIONARY);
      } else if (hasVertexDictionary()) {
        simpleLabelTransaction = graphTransactions.getTransactions()
          // map the simple integer-like labels
          .map(new ElementLabelEncoder<G, V, E>(
            hasVertexDictionary(), hasEdgeDictionary()))
          .withBroadcastSet(vertexDictionary,
            BroadcastNames.VERTEX_DICTIONARY);
      } else {
        simpleLabelTransaction = graphTransactions.getTransactions()
          // map the simple integer-like labels
          .map(new ElementLabelEncoder<G, V, E>(
            hasVertexDictionary(), hasEdgeDictionary()))
          .withBroadcastSet(edgeDictionary,
            BroadcastNames.EDGE_DICTIONARY);
      }
      // write the TLF format adjusted graphs to file
      simpleLabelTransaction
        .writeAsFormattedText(getTLFPath(),
          new TLFFileFormat<G, V, E>());
    // if there was no dictionary used the graphs can simply be written
    } else {
      graphTransactions.getTransactions()
        .writeAsFormattedText(getTLFPath(),
          new TLFFileFormat<G, V, E>());
    }
  }
}
