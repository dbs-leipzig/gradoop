/**
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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.tlf.functions.Dictionary;
import org.gradoop.flink.io.impl.tlf.functions.DictionaryEntry;
import org.gradoop.flink.io.impl.tlf.functions.EdgeLabelDecoder;
import org.gradoop.flink.io.impl.tlf.functions.GraphTransactionFromText;
import org.gradoop.flink.io.impl.tlf.functions.TLFFileFormat;
import org.gradoop.flink.io.impl.tlf.functions.VertexLabelDecoder;
import org.gradoop.flink.io.impl.tlf.inputformats.TLFInputFormat;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Creates an EPGM instance from one TLF file. The exact format is
 * documented in
 * {@link TLFFileFormat}.
 */
public class TLFDataSource extends TLFBase implements DataSource {

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param tlfPath tlf data file
   * @param config Gradoop Flink configuration
   */
  public TLFDataSource(String tlfPath, GradoopFlinkConfig config) {
    super(tlfPath, "", "", config);
  }

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param tlfPath tlf data file
   * @param tlfVertexDictionaryPath tlf vertex dictionary file
   * @param tlfEdgeDictionaryPath tlf edge dictionary file
   * @param config Gradoop Flink configuration
   */
  public TLFDataSource(String tlfPath, String tlfVertexDictionaryPath,
    String tlfEdgeDictionaryPath, GradoopFlinkConfig config) {
    super(tlfPath, tlfVertexDictionaryPath, tlfEdgeDictionaryPath, config);
    ExecutionEnvironment env = config.getExecutionEnvironment();
    if (hasVertexDictionary()) {
      setVertexDictionary(env
        .readHadoopFile(new TextInputFormat(), LongWritable.class, Text
          .class, getTLFVertexDictionaryPath())
          .map(new DictionaryEntry())
          .reduceGroup(new Dictionary()));
    }
    if (hasEdgeDictionary()) {
      setEdgeDictionary(env
        .readHadoopFile(new TextInputFormat(), LongWritable.class, Text
          .class, getTLFEdgeDictionaryPath())
          .map(new DictionaryEntry())
          .reduceGroup(new Dictionary()));
    }
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    DataSet<GraphTransaction> transactions;
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    // load tlf graphs from file
    transactions = env.readHadoopFile(
      new TLFInputFormat(), LongWritable.class, Text.class, getTLFPath())
      .map(new GraphTransactionFromText(
        getConfig().getGraphHeadFactory(),
        getConfig().getVertexFactory(),
        getConfig().getEdgeFactory()));

    // map the integer valued labels to strings from dictionary
    if (hasVertexDictionary()) {
      transactions = transactions
        .map(new VertexLabelDecoder())
        .withBroadcastSet(
          getVertexDictionary(), TLFConstants.VERTEX_DICTIONARY);
    }
    if (hasEdgeDictionary()) {
      transactions = transactions
        .map(new EdgeLabelDecoder())
        .withBroadcastSet(
          getEdgeDictionary(), TLFConstants.EDGE_DICTIONARY);
    }
    return getConfig().getGraphCollectionFactory().fromTransactions(transactions);
  }
}
