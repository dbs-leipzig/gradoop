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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.gradoop.flink.io.impl.tlf.functions.GraphTransactionFromTLFGraph;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.tlf.functions.Dictionary;
import org.gradoop.flink.io.impl.tlf.functions.DictionaryEntry;
import org.gradoop.flink.io.impl.tlf.functions.EdgeLabelDecoder;
import org.gradoop.flink.io.impl.tlf.functions.TLFFileFormat;
import org.gradoop.flink.io.impl.tlf.functions.TLFGraphFromText;
import org.gradoop.flink.io.impl.tlf.functions.VertexLabelDecoder;
import org.gradoop.flink.io.impl.tlf.inputformats.TLFInputFormat;
import org.gradoop.flink.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

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
    return GraphCollection.fromTransactions(getGraphTransactions());
  }

  @Override
  public GraphTransactions getGraphTransactions() throws IOException {
    DataSet<TLFGraph> graphs;
    DataSet<GraphTransaction> transactions;
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    // load tlf graphs from file
    graphs = env.readHadoopFile(
      new TLFInputFormat(), LongWritable.class, Text.class, getTLFPath())
      .map(new TLFGraphFromText());

    // map the tlf graph to transactions
    transactions = graphs
      .map(new GraphTransactionFromTLFGraph(
        getConfig().getGraphHeadFactory(),
        getConfig().getVertexFactory(),
        getConfig().getEdgeFactory()))
        .returns(GraphTransaction.getTypeInformation(getConfig()));

    // map the integer valued labels to strings from dictionary
    if (hasVertexDictionary()) {
      transactions = transactions
        .map(new VertexLabelDecoder())
        .withBroadcastSet(
          getVertexDictionary(), VertexLabelDecoder.VERTEX_DICTIONARY);
    }
    if (hasEdgeDictionary()) {
      transactions = transactions
        .map(new EdgeLabelDecoder())
        .withBroadcastSet(
          getEdgeDictionary(), EdgeLabelDecoder.EDGE_DICTIONARY);
    }
    return new GraphTransactions(transactions, getConfig());
  }

  /**
   * Reads the input as dataset of TLFGraphs.
   *
   * @return tlf graphs
   */
  public DataSet<TLFGraph> getTLFGraphs() throws IOException {
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    return env.readHadoopFile(new TLFInputFormat(),
      LongWritable.class, Text.class, getTLFPath())
      .map(new TLFGraphFromText());
  }
}
