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
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.nested.datarepresentation.functions.VertexOrEdgeArray;
import org.gradoop.flink.model.impl.nested.datarepresentation.functions.WriteGradoopIdGradoopIdSet;
import org.gradoop.flink.model.impl.nested.utils.FileSystemUtils;
import org.gradoop.flink.model.impl.nested.utils.RepresentationUtils;

import java.io.IOException;
import java.util.Set;

/**
 * Writes the relation graphs in a convenience format, determining the subgraphs that have to
 * be extracted.
 */
public class ByteArraysDataSink implements DataSink {

  /**
   * Project GraphTransaction into a set of edges
   */
  private static final VertexOrEdgeArray vertexReduction;
  /**
   * Project GraphTransaction into a set of vertices
   */
  private static final VertexOrEdgeArray edgeReduction;
  /**
   * File where to write the vertices
   */
  private final String vertexFile;
  /**
   * File where to write the edges
   */
  private final String edgeFile;
  /**
   * If the files have to be overwritten
   */
  private final boolean overwrite;

  static {
    vertexReduction = new VertexOrEdgeArray(true);
    edgeReduction = new VertexOrEdgeArray(false);
  }

  /**
   * Default constructor
   * @param baseName    Base file name identifying the dataset
   * @param overwrite     If the files have to be overwritten
   */
  public ByteArraysDataSink(String baseName, boolean overwrite) {
    this.vertexFile = FileSystemUtils.generateVertexFile(baseName);
    this.edgeFile = FileSystemUtils.generateVertexFile(baseName);
    this.overwrite = overwrite;
  }

  @Override
  public void write(GraphTransactions graphTransactions, boolean overWrite) throws IOException {
    FileSystem.WriteMode wm = FileSystemUtils.overwrite(overWrite);
    DataSet<Tuple2<GradoopId, Set<GradoopId>>> vertices =
      graphTransactions.getTransactions()
        .map(vertexReduction);
    DataSet<Tuple2<GradoopId, Set<GradoopId>>> edges =
      graphTransactions.getTransactions()
        .map(edgeReduction);
    vertices.write(new WriteGradoopIdGradoopIdSet(),vertexFile,wm);
    edges.write(new WriteGradoopIdGradoopIdSet(),edgeFile,wm);
  }

  // Bogus methods

  @Override
  public void write(LogicalGraph logicalGraph) throws IOException {
    write(RepresentationUtils.toTransaction(logicalGraph),overwrite);
  }

  @Override
  public void write(GraphCollection graphCollection) throws IOException {
    write(RepresentationUtils.toTransaction(graphCollection),overwrite);
  }

  @Override
  public void write(GraphTransactions graphTransactions) throws IOException {
    write(graphTransactions,overwrite);
  }

  @Override
  public void write(LogicalGraph logicalGraph, boolean overWrite) throws IOException {
    write(RepresentationUtils.toTransaction(logicalGraph),overWrite);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overWrite) throws IOException {
    write(RepresentationUtils.toTransaction(graphCollection),overWrite);
  }


}
