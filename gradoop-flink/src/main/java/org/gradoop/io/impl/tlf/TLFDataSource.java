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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.tlf.inputformats.TLFInputFormat;
import org.gradoop.io.impl.tlf.functions.TLFGraphCollectionToGraphTransactions;
import org.gradoop.io.impl.tlf.functions.GraphTransactionsToTLFFile;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.combination.ReduceCombination;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Creates an EPGM instance from one TLF file. The exact format is
 * documented in
 * {@link GraphTransactionsToTLFFile}.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class TLFDataSource
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends TLFBase<G, V, E>
  implements DataSource<G, V, E> {

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param tlfPath tlf data file
   * @param config Gradoop Flink configuration
   */
  public TLFDataSource(String tlfPath, GradoopFlinkConfig<G, V, E>
    config) {
    super(tlfPath, config);
  }

  @Override
  public LogicalGraph<G, V, E> getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination<G, V, E>());
  }

  @Override
  public GraphCollection<G, V, E> getGraphCollection() {
    return GraphCollection.fromTransactions(getGraphTransactions());
  }

  @Override
  public GraphTransactions<G, V, E> getGraphTransactions() {
    DataSet<GraphTransaction<G, V, E>> transactions = null;
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    // create the mapper
    TLFGraphCollectionToGraphTransactions<G, V, E>
      tlfCollectionToGraphTransactions = new
      TLFGraphCollectionToGraphTransactions<G, V, E>(getConfig()
        .getGraphHeadFactory(), getConfig().getVertexFactory(), getConfig()
          .getEdgeFactory());
    // get the mapper's produced type
    TypeInformation<GraphTransaction<G, V, E>> typeInformation =
      tlfCollectionToGraphTransactions
        .getProducedType();
    try {
      // map the file's content to transactions
      transactions = env.readHadoopFile(new TLFInputFormat(),
        LongWritable.class, Text.class, getTLFPath())
          .flatMap(tlfCollectionToGraphTransactions)
          .returns(typeInformation);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    return new GraphTransactions<G, V, E>(transactions, getConfig());
  }
}
