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

package org.gradoop.flink.model.impl.nested.datarepresentation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.nested.HadoopDataSource;
import org.gradoop.flink.model.impl.nested.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.datarepresentation.functions.ReadGradoopIdGradoopIdSet;
import org.gradoop.flink.model.impl.nested.datarepresentation.functions.UnnestGidGid;
import org.gradoop.flink.model.impl.nested.utils.FileSystemUtils;
import org.gradoop.flink.model.impl.nested.utils.RepresentationUtils;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Reads the source where the graph is stored
 */
public class ByteArraysDataSource extends HadoopDataSource<IdGraphDatabase> {

  private final ReduceCombination reductor;

  private final String vertexFile;

  private final String edgeFile;

  private LogicalGraph dataLake;

  public ByteArraysDataSource(GradoopFlinkConfig conf, String baseFile) {
    super(conf);
    reductor = new ReduceCombination();
    vertexFile = FileSystemUtils.generateVertexFile(baseFile);
    edgeFile = FileSystemUtils.generateEdgeFile(baseFile);
  }

  public LogicalGraph getDataLake() {
    return dataLake;
  }

  public void setDataLake(LogicalGraph dataLake) {
    this.dataLake = dataLake;
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    return getGraphCollection().reduce(reductor);
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    return getDefaultRepresentation().asGraphCollection(dataLake);
  }

  @Override
  public GraphTransactions getGraphTransactions() throws IOException {
    return RepresentationUtils.toTransaction(getGraphCollection());
  }

  @Override
  public IdGraphDatabase getDefaultRepresentation() {
    DataSet<Tuple2<GradoopId, GradoopId>>
      vertices = FileSystemUtils.hadoopFile(this, new ReadGradoopIdGradoopIdSet(), vertexFile)
      .flatMap(new UnnestGidGid());
    DataSet<Tuple2<GradoopId, GradoopId>>
      edges = FileSystemUtils.hadoopFile(this, new ReadGradoopIdGradoopIdSet(), edgeFile)
      .flatMap(new UnnestGidGid());
    DataSet<GradoopId> heads = FileSystemUtils
      .hadoopFile(this,new ReadGradoopIdGradoopIdSet(),vertexFile)
      .map(new Value0Of2<>());
    return new IdGraphDatabase(heads,vertices,edges);

  }
}
