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

package org.gradoop.examples.statistics;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsReader;
import org.gradoop.flink.model.impl.operators.statistics.DistinctEdgePropertyValuesByLabelAndPropertyName;
import org.gradoop.flink.model.impl.operators.statistics.DistinctEdgePropertyValuesByPropertyName;
import org.gradoop.flink.model.impl.operators.statistics.DistinctSourceIds;
import org.gradoop.flink.model.impl.operators.statistics.DistinctSourceIdsByEdgeLabel;
import org.gradoop.flink.model.impl.operators.statistics.DistinctTargetIds;
import org.gradoop.flink.model.impl.operators.statistics.DistinctTargetIdsByEdgeLabel;
import org.gradoop.flink.model.impl.operators.statistics.DistinctVertexPropertyValuesByLabelAndPropertyName;
import org.gradoop.flink.model.impl.operators.statistics.DistinctVertexPropertyValuesByPropertyName;
import org.gradoop.flink.model.impl.operators.statistics.EdgeCount;
import org.gradoop.flink.model.impl.operators.statistics.EdgeLabelDistribution;
import org.gradoop.flink.model.impl.operators.statistics.OutgoingVertexDegreeDistribution;
import org.gradoop.flink.model.impl.operators.statistics.SourceLabelAndEdgeLabelDistribution;
import org.gradoop.flink.model.impl.operators.statistics.TargetLabelAndEdgeLabelDistribution;
import org.gradoop.flink.model.impl.operators.statistics.VertexCount;
import org.gradoop.flink.model.impl.operators.statistics.VertexDegreeDistribution;
import org.gradoop.flink.model.impl.operators.statistics.VertexLabelDistribution;

/**
 * Computes several statistics of a given EPGM graph.
 */
public class Statistics extends AbstractRunner implements ProgramDescription {

  /**
   * Computes several statistics of the specified graph.
   *
   * @param args args[0] = input dir, args[1] output dir
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String inputDir = args[0];
    String outputDir = args[1];

    LogicalGraph graph = readLogicalGraph(inputDir);

    //----------------------------------------------------------------------------------------------
    // Vertex Count
    //----------------------------------------------------------------------------------------------
    new VertexCount()
      .execute(graph)
      .map(new ObjectTo1<>())
      .writeAsCsv(outputDir + GraphStatisticsReader.FILE_VERTEX_COUNT,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Edge Count
    //----------------------------------------------------------------------------------------------
    new EdgeCount()
      .execute(graph)
      .map(new ObjectTo1<>())
      .writeAsCsv(outputDir + GraphStatisticsReader.FILE_EDGE_COUNT,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Vertex Label Distribution
    //----------------------------------------------------------------------------------------------
    new VertexLabelDistribution()
      .execute(graph)
      .writeAsCsv(outputDir + GraphStatisticsReader.FILE_VERTEX_COUNT_BY_LABEL,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Edge Label Distribution
    //----------------------------------------------------------------------------------------------
    new EdgeLabelDistribution()
      .execute(graph)
      .writeAsCsv(outputDir + GraphStatisticsReader.FILE_EDGE_COUNT_BY_LABEL,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Vertex Degree Distribution
    //----------------------------------------------------------------------------------------------
    new VertexDegreeDistribution()
      .execute(graph)
      .writeAsCsv(outputDir + "vertex_degree_distribution")
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Outgoing Vertex Degree Distribution
    //----------------------------------------------------------------------------------------------
    new OutgoingVertexDegreeDistribution()
      .execute(graph)
      .writeAsCsv(outputDir + "outgoing_vertex_degree_distribution")
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Incoming Vertex Degree Distribution
    //----------------------------------------------------------------------------------------------
    new OutgoingVertexDegreeDistribution()
      .execute(graph)
      .writeAsCsv(outputDir + "incoming_vertex_degree_distribution")
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Distinct Source Vertices
    //----------------------------------------------------------------------------------------------
    new DistinctSourceIds()
      .execute(graph)
      .map(new ObjectTo1<>())
      .writeAsCsv(outputDir + GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Distinct Target Vertices
    //----------------------------------------------------------------------------------------------
    new DistinctTargetIds()
      .execute(graph)
      .map(new ObjectTo1<>())
      .writeAsCsv(outputDir + GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Distinct Source Vertices by Edge Label
    //----------------------------------------------------------------------------------------------
    new DistinctSourceIdsByEdgeLabel()
      .execute(graph)
      .writeAsCsv(
        outputDir + GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT_BY_EDGE_LABEL,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Distinct Target Vertices by Edge Label
    //----------------------------------------------------------------------------------------------
    new DistinctTargetIdsByEdgeLabel()
      .execute(graph)
      .writeAsCsv(
        outputDir + GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT_BY_EDGE_LABEL,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Source Label and Edge Label Distribution
    //----------------------------------------------------------------------------------------------
    new SourceLabelAndEdgeLabelDistribution()
      .execute(graph)
      .map(value -> Tuple3.of(value.f0.f0, value.f0.f1, value.f1))
      .returns(new TypeHint<Tuple3<String, String, Long>>() { })
      .writeAsCsv(
        outputDir + GraphStatisticsReader.FILE_EDGE_COUNT_BY_SOURCE_VERTEX_AND_EDGE_LABEL,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Target Label and Edge Label Distribution
    //----------------------------------------------------------------------------------------------
    new TargetLabelAndEdgeLabelDistribution()
      .execute(graph)
      .map(value -> Tuple3.of(value.f0.f0, value.f0.f1, value.f1))
      .returns(new TypeHint<Tuple3<String, String, Long>>() { })
      .writeAsCsv(
        outputDir + GraphStatisticsReader.FILE_EDGE_COUNT_BY_TARGET_VERTEX_AND_EDGE_LABEL,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Distinct edge PropertyValues by Label - PropertyName pairs
    //----------------------------------------------------------------------------------------------
    new DistinctEdgePropertyValuesByLabelAndPropertyName()
      .execute(graph)
      .map(value -> Tuple3.of(value.f0.f0, value.f0.f1, value.f1))
      .returns(new TypeHint<Tuple3<String, String, Long>>() { })
      .writeAsCsv(
        outputDir +
          GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTY_VALUES_BY_LABEL_AND_PROPERTY_KEY,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Distinct vertex PropertyValues by Label - PropertyName pairs
    //----------------------------------------------------------------------------------------------
    new DistinctVertexPropertyValuesByLabelAndPropertyName()
      .execute(graph)
      .map(value -> Tuple3.of(value.f0.f0, value.f0.f1, value.f1))
      .returns(new TypeHint<Tuple3<String, String, Long>>() { })
      .writeAsCsv(
        outputDir +
          GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTY_VALUES_BY_LABEL_AND_PROPERTY_KEY,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Distinct edge PropertyValues by PropertyName
    //----------------------------------------------------------------------------------------------
    new DistinctEdgePropertyValuesByPropertyName()
      .execute(graph)
      .writeAsCsv(
        outputDir +
          GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTY_VALUES_BY_PROPERTY_KEY,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    //----------------------------------------------------------------------------------------------
    // Distinct vertex PropertyValues by PropertyName
    //----------------------------------------------------------------------------------------------
    new DistinctVertexPropertyValuesByPropertyName()
      .execute(graph)
      .writeAsCsv(
        outputDir +
          GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTY_VALUES_BY_PROPERTY_KEY,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    getExecutionEnvironment().execute();
  }

  @Override
  public String getDescription() {
    return "Graph Statistics";
  }
}
