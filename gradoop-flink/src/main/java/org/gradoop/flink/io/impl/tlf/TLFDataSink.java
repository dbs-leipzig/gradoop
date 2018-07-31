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
package org.gradoop.flink.io.impl.tlf;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.tlf.functions.EdgeLabelList;
import org.gradoop.flink.io.impl.tlf.functions.ElementLabelEncoder;
import org.gradoop.flink.io.impl.tlf.functions.TLFDictionaryFileFormat;
import org.gradoop.flink.io.impl.tlf.functions.TLFDictionaryMapGroupReducer;
import org.gradoop.flink.io.impl.tlf.functions.TLFFileFormat;
import org.gradoop.flink.io.impl.tlf.functions.VertexLabelList;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

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
  public void write(LogicalGraph logicalGraph, boolean overwrite) throws IOException {
    write(logicalGraph.getConfig().getGraphCollectionFactory().fromGraph(logicalGraph), overwrite);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overWrite) throws IOException {
    FileSystem.WriteMode writeMode =
      overWrite ? FileSystem.WriteMode.OVERWRITE :  FileSystem.WriteMode.NO_OVERWRITE;

    DataSet<GraphTransaction> graphTransactions = graphCollection.getGraphTransactions();
    DataSet<GraphTransaction> simpleLabelTransaction;
    DataSet<Map<String, Integer>> vertexDictionary = null;
    DataSet<Map<String, Integer>> edgeDictionary = null;
    // if the graph transaction vertex labels are set by a dictionary
    if (hasVertexDictionary()) {

      vertexDictionary = graphTransactions
        // get a vertex dictionary for each transaction
        .flatMap(new VertexLabelList())
        .distinct()
        // reduce them to one dictionary without duplicates
        .reduceGroup(new TLFDictionaryMapGroupReducer());
      // write the vertex dictionary
      vertexDictionary
        .writeAsFormattedText(
          getTLFVertexDictionaryPath(),
          writeMode,
          new TLFDictionaryFileFormat());
    }

    if (hasEdgeDictionary()) {
      edgeDictionary = graphTransactions
        // get an edge dictionary for each transaction
        .flatMap(new EdgeLabelList())
        .distinct()
        // reduce them to one dictionary without duplicates
        .reduceGroup(new TLFDictionaryMapGroupReducer());
      // write the edge dictionary
      edgeDictionary
        .writeAsFormattedText(
          getTLFEdgeDictionaryPath(),
          writeMode,
          new TLFDictionaryFileFormat());
    }

    if (hasVertexDictionary() || hasEdgeDictionary()) {
      if (hasVertexDictionary() && hasEdgeDictionary()) {
        simpleLabelTransaction = graphTransactions
          // map the simple integer-like labels
          .map(new ElementLabelEncoder(
            hasVertexDictionary(), hasEdgeDictionary()))
          .withBroadcastSet(vertexDictionary,
            TLFConstants.VERTEX_DICTIONARY)
          .withBroadcastSet(edgeDictionary,
            TLFConstants.EDGE_DICTIONARY);
      } else if (hasVertexDictionary()) {
        simpleLabelTransaction = graphTransactions
          // map the simple integer-like labels
          .map(new ElementLabelEncoder(
            hasVertexDictionary(), hasEdgeDictionary()))
          .withBroadcastSet(vertexDictionary,
            TLFConstants.VERTEX_DICTIONARY);
      } else {
        simpleLabelTransaction = graphTransactions
          // map the simple integer-like labels
          .map(new ElementLabelEncoder(
            hasVertexDictionary(), hasEdgeDictionary()))
          .withBroadcastSet(edgeDictionary,
            TLFConstants.EDGE_DICTIONARY);
      }
      // write the TLF format adjusted graphs to file
      simpleLabelTransaction
        .writeAsFormattedText(getTLFPath(), writeMode, new TLFFileFormat());
      // if there was no dictionary used the graphs can simply be written
    } else {
      graphTransactions
        .writeAsFormattedText(getTLFPath(), writeMode, new TLFFileFormat());
    }
  }
}
