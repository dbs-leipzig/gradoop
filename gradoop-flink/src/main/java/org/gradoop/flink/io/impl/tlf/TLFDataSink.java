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

package org.gradoop.flink.io.impl.tlf;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.tlf.constants.BroadcastNames;
import org.gradoop.flink.io.impl.tlf.functions.EdgeLabelList;
import org.gradoop.flink.io.impl.tlf.functions.ElementLabelEncoder;
import org.gradoop.flink.io.impl.tlf.functions.TLFDictionaryFileFormat;
import org.gradoop.flink.io.impl.tlf.functions.TLFFileFormat;
import org.gradoop.flink.io.impl.tlf.functions.VertexLabelList;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.io.impl.tlf.functions.TLFDictionaryMapGroupReducer;
import org.gradoop.flink.model.impl.GraphCollection;

import java.io.IOException;
import java.util.Map;


/**
 * Writes an EPGM representation into one TLF file. The format
 * is documented at {@link TLFFileFormat}.
 */
public class TLFDataSink extends TLFBase implements DataSink {

  /**
   * Creates a new data sink. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param tlfPath tlf data file
   * @param config Gradoop Flink configuration
   */
  public TLFDataSink(String tlfPath, GradoopFlinkConfig config) {
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
    String tlfEdgeDictionaryPath, GradoopFlinkConfig config) {
    super(tlfPath, tlfVertexDictionaryPath, tlfEdgeDictionaryPath, config);
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

    DataSet<GraphTransaction> simpleLabelTransaction;
    DataSet<Map<String, Integer>> vertexDictionary = null;
    DataSet<Map<String, Integer>> edgeDictionary = null;
    // if the graph transaction vertex labels are set by a dictionary
    if (hasVertexDictionary()) {
      vertexDictionary = graphTransactions.getTransactions()
        // get a vertex dictionary for each transaction
        .flatMap(new VertexLabelList())
        .distinct()
        // reduce them to one dictionary without duplicates
        .reduceGroup(new TLFDictionaryMapGroupReducer());
      // write the vertex dictionary
      vertexDictionary
        .writeAsFormattedText(
          getTLFVertexDictionaryPath(), writeMode, new TLFDictionaryFileFormat());
    }

    if (hasEdgeDictionary()) {
      edgeDictionary = graphTransactions.getTransactions()
        // get an edge dictionary for each transaction
        .flatMap(new EdgeLabelList())
        .distinct()
        // reduce them to one dictionary without duplicates
        .reduceGroup(new TLFDictionaryMapGroupReducer());
      // write the edge dictionary
      edgeDictionary
        .writeAsFormattedText(getTLFEdgeDictionaryPath(), writeMode, new TLFDictionaryFileFormat());
    }


    if (hasVertexDictionary() || hasEdgeDictionary()) {
      if (hasVertexDictionary() && hasEdgeDictionary()) {
        simpleLabelTransaction = graphTransactions.getTransactions()
          // map the simple integer-like labels
          .map(new ElementLabelEncoder(
            hasVertexDictionary(), hasEdgeDictionary()))
          .withBroadcastSet(vertexDictionary,
            BroadcastNames.VERTEX_DICTIONARY)
          .withBroadcastSet(edgeDictionary,
            BroadcastNames.EDGE_DICTIONARY);
      } else if (hasVertexDictionary()) {
        simpleLabelTransaction = graphTransactions.getTransactions()
          // map the simple integer-like labels
          .map(new ElementLabelEncoder(
            hasVertexDictionary(), hasEdgeDictionary()))
          .withBroadcastSet(vertexDictionary,
            BroadcastNames.VERTEX_DICTIONARY);
      } else {
        simpleLabelTransaction = graphTransactions.getTransactions()
          // map the simple integer-like labels
          .map(new ElementLabelEncoder(
            hasVertexDictionary(), hasEdgeDictionary()))
          .withBroadcastSet(edgeDictionary,
            BroadcastNames.EDGE_DICTIONARY);
      }
      // write the TLF format adjusted graphs to file
      simpleLabelTransaction
        .writeAsFormattedText(getTLFPath(), writeMode, new TLFFileFormat());
      // if there was no dictionary used the graphs can simply be written
    } else {
      graphTransactions.getTransactions()
        .writeAsFormattedText(getTLFPath(), writeMode, new TLFFileFormat());
    }
  }
}
