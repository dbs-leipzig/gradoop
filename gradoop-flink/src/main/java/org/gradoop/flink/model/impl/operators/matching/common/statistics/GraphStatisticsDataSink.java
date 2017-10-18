/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.matching.common.statistics;

import java.io.IOException;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.CypherPatternMatching;
import org.gradoop.flink.model.impl.operators.statistics.DistinctEdgeProperties;
import org.gradoop.flink.model.impl.operators.statistics.DistinctEdgePropertiesByLabel;
import org.gradoop.flink.model.impl.operators.statistics.DistinctSourceIds;
import org.gradoop.flink.model.impl.operators.statistics.DistinctSourceIdsByEdgeLabel;
import org.gradoop.flink.model.impl.operators.statistics.DistinctTargetIds;
import org.gradoop.flink.model.impl.operators.statistics.DistinctTargetIdsByEdgeLabel;
import org.gradoop.flink.model.impl.operators.statistics.DistinctVertexProperties;
import org.gradoop.flink.model.impl.operators.statistics.DistinctVertexPropertiesByLabel;
import org.gradoop.flink.model.impl.operators.statistics.EdgeCount;
import org.gradoop.flink.model.impl.operators.statistics.EdgeLabelDistribution;
import org.gradoop.flink.model.impl.operators.statistics.SourceLabelAndEdgeLabelDistribution;
import org.gradoop.flink.model.impl.operators.statistics.TargetLabelAndEdgeLabelDistribution;
import org.gradoop.flink.model.impl.operators.statistics.VertexCount;
import org.gradoop.flink.model.impl.operators.statistics.VertexLabelDistribution;

/**
 * Estimates all graph statistics read by subclasses of {@link GraphStatisticsReader}.
 * The statistics are currently restricted to the ones used by the cost plan
 * estimator of the cypher pattern matching operator {@link CypherPatternMatching}.
 * Caution: This data sink ONLY knows how to process {@link LogicalGraph}s!
 */
public class GraphStatisticsDataSink implements DataSink {

  /**
   * Directory/HDFS directory name for statistics output
   */
  private final String path;

  /**
   * Default constructor
   * @param path file path in a local filesystem or HDFS
   */
  public GraphStatisticsDataSink(final String path) {
    this.path = path;
  }
  /* (non-Javadoc)
   * @see org.gradoop.flink.io.api.DataSink#write(org.gradoop.flink.model.api.epgm.LogicalGraph)
   */
  @Override
  public void write(final LogicalGraph logicalGraph) throws IOException {
    write(logicalGraph, false);
  }


  /* (non-Javadoc)
   * @see org.gradoop.flink.io.api.DataSink#write(org.gradoop.flink.model.api.epgm.LogicalGraph, boolean)
   */
  @Override
  public void write(final LogicalGraph graph, final boolean overwrite) throws IOException {

    // VertexCountRunner.main(args)
    writeCSV(
        new VertexCount()
        .execute(graph)
        .map(new ObjectTo1<>()),
        GraphStatisticsReader.FILE_VERTEX_COUNT,
        overwrite);
    // EdgeCountRunner.main(args)
    writeCSV(
        new EdgeCount()
        .execute(graph)
        .map(new ObjectTo1<>()),
        GraphStatisticsReader.FILE_EDGE_COUNT,
        overwrite);
    // VertexLabelDistributionRunner.main(args)
    writeCSV(
        new VertexLabelDistribution()
        .execute(graph),
        GraphStatisticsReader.FILE_VERTEX_COUNT_BY_LABEL,
        overwrite);
    //    EdgeLabelDistributionRunner.main(args);
    writeCSV(
        new EdgeLabelDistribution()
        .execute(graph) ,
        GraphStatisticsReader.FILE_EDGE_COUNT_BY_LABEL,
        overwrite);
    //    DistinctSourceVertexCountRunner.main(args);
    writeCSV(
        new DistinctSourceIds()
        .execute(graph)
        .map(new ObjectTo1<>()),
        GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT,
        overwrite);
    //    DistinctTargetVertexCountRunner.main(args);
    writeCSV(
        new DistinctTargetIds()
        .execute(graph)
        .map(new ObjectTo1<>()),
        GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT,
        overwrite);
    //    DistinctSourceVertexCountByEdgeLabelRunner.main(args);
    writeCSV(
        new DistinctSourceIdsByEdgeLabel()
        .execute(graph),
        GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT_BY_EDGE_LABEL,
        overwrite);
    //    DistinctTargetVertexCountByEdgeLabelRunner.main(args);
    writeCSV(
        new DistinctTargetIdsByEdgeLabel()
        .execute(graph),
        GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT_BY_EDGE_LABEL,
        overwrite);
    //    SourceAndEdgeLabelDistributionRunner.main(args);
    writeCSV(
        new SourceLabelAndEdgeLabelDistribution()
        .execute(graph)
        .map(value -> Tuple3.of(value.f0.f0, value.f0.f1, value.f1))
        .returns(new TypeHint<Tuple3<String, String, Long>>() { }),
        GraphStatisticsReader.FILE_EDGE_COUNT_BY_SOURCE_VERTEX_AND_EDGE_LABEL,
        overwrite);
    //    TargetAndEdgeLabelDistributionRunner.main(args);
    writeCSV(
        new TargetLabelAndEdgeLabelDistribution()
        .execute(graph)
        .map(value -> Tuple3.of(value.f0.f0, value.f0.f1, value.f1))
        .returns(new TypeHint<Tuple3<String, String, Long>>() { }),
        GraphStatisticsReader.FILE_EDGE_COUNT_BY_TARGET_VERTEX_AND_EDGE_LABEL,
        overwrite);
    //    DistinctEdgePropertiesByLabelRunner.main(args);
    writeCSV(
        new DistinctEdgePropertiesByLabel()
        .execute(graph)
        .map(value -> Tuple3.of(value.f0.f0, value.f0.f1, value.f1))
        .returns(new TypeHint<Tuple3<String, String, Long>>() { }),
        GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTIES_BY_LABEL,
        overwrite);
    //    DistinctVertexPropertiesByLabelRunner.main(args);
    writeCSV(
        new DistinctVertexPropertiesByLabel()
        .execute(graph)
        .map(value -> Tuple3.of(value.f0.f0, value.f0.f1, value.f1))
        .returns(new TypeHint<Tuple3<String, String, Long>>() { }),
        GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTIES_BY_LABEL,
        overwrite);
    //    DistinctEdgePropertiesRunner.main(args);
    writeCSV(
        new DistinctEdgeProperties()
        .execute(graph),
        GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTIES,
        overwrite);
    //    DistinctVertexPropertiesRunner.main(args);
    writeCSV(
        new DistinctVertexProperties()
        .execute(graph),
        GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTIES,
        overwrite);
  }

  /**
   * Write tuples as CSV files into a preconfigured file in a path.
   * @param tuples tuples to write (fields separated by GraphStatisticsReader.TOKEN_SEPARATOR)
   * @param fileName file/hdfs file name to write to
   * @param overWrite should the target file be overwritten if it already exists?
   * @param <T> type of tuple, necessary to satisfy the Java compiler
   */
  private <T extends Tuple> void writeCSV(final DataSet<T> tuples,
      final String fileName, final boolean overWrite) {
    tuples.writeAsCsv(
        appendSeparator(this.path) + fileName,
        System.lineSeparator(),
        GraphStatisticsReader.TOKEN_SEPARATOR,
        overWrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE)
      .setParallelism(1);
  }
  /**
   * Appends a file separator to the given directory (if not already existing).
   *
   * @param directory directory
   * @return directory with OS specific file separator
   */
  private String appendSeparator(final String directory) {
    final String fileSeparator = System.getProperty("file.separator");
    String result = directory;
    if (!directory.endsWith(fileSeparator)) {
      result = directory + fileSeparator;
    }
    return result;
  }
  /* (non-Javadoc)
   * @see org.gradoop.flink.io.api.DataSink#write(org.gradoop.flink.model.api.epgm.GraphCollection)
   */
  @Override
  public void write(final GraphCollection graphCollection) throws IOException {
    write(graphCollection, false);
  }
  /* (non-Javadoc)
   * @see org.gradoop.flink.io.api.DataSink#write(org.gradoop.flink.model.api.epgm.GraphCollection, boolean)
   */
  @Override
  public void write(final GraphCollection graphCollection, final boolean overWrite)
      throws IOException {
    throw new UnsupportedOperationException("This sink can only process instances of LogicalGraph");
  }

}
