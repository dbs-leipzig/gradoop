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
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.DistinctEdgePropertyValuesByLabelAndPropertyName;
import org.gradoop.flink.model.impl.operators.statistics.DistinctVertexPropertyValuesByLabelAndPropertyName;
import org.gradoop.flink.model.impl.operators.statistics.DistinctSourceIds;
import org.gradoop.flink.model.impl.operators.statistics.DistinctSourceIdsByEdgeLabel;
import org.gradoop.flink.model.impl.operators.statistics.DistinctTargetIds;
import org.gradoop.flink.model.impl.operators.statistics.DistinctTargetIdsByEdgeLabel;
import org.gradoop.flink.model.impl.operators.statistics.EdgeCount;
import org.gradoop.flink.model.impl.operators.statistics.EdgeLabelDistribution;
import org.gradoop.flink.model.impl.operators.statistics.VertexCount;
import org.gradoop.flink.model.impl.operators.statistics.VertexLabelDistribution;

/**
 * Build a single file containing basic graph statistics.
 */
public class BuildStatisticsFile extends AbstractRunner implements ProgramDescription {

  /**
   * Computes several statistics of the specified graph.
   *
   * @param args args[0] = input dir, args[1] output file
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String inputDir = args[0];
    String outputFile = args[1];

    LogicalGraph graph = readLogicalGraph(inputDir);

    TypeHint<Tuple2<String, Long>> typeHint = new TypeHint<Tuple2<String, Long>>() { };

    //----------------------------------------------------------------------------------------------
    // Vertex Count
    //----------------------------------------------------------------------------------------------
    DataSet<Tuple2<String, Long>> vertexCount = new VertexCount()
      .execute(graph)
      .map(c -> Tuple2.of("vertices", c))
      .returns(typeHint);

    //----------------------------------------------------------------------------------------------
    // Edge Count
    //----------------------------------------------------------------------------------------------
    DataSet<Tuple2<String, Long>> edgeCount = new EdgeCount()
      .execute(graph)
      .map(c -> Tuple2.of("edges", c))
      .returns(typeHint);

    //----------------------------------------------------------------------------------------------
    // Vertex Label Distribution
    //----------------------------------------------------------------------------------------------
    DataSet<Tuple2<String, Long>> vertexLabelDistribution = new VertexLabelDistribution()
      .execute(graph)
      .map(withCount -> Tuple2.of("vertices." + withCount.getObject(), withCount.getCount()))
      .returns(typeHint);

    //----------------------------------------------------------------------------------------------
    // Edge Label Distribution
    //----------------------------------------------------------------------------------------------
    DataSet<Tuple2<String, Long>> edgeLabelDistribution = new EdgeLabelDistribution()
      .execute(graph)
      .map(withCount -> Tuple2.of("edges." + withCount.getObject(), withCount.getCount()))
      .returns(typeHint);

    //----------------------------------------------------------------------------------------------
    // Distinct Source Vertices
    //----------------------------------------------------------------------------------------------
    DataSet<Tuple2<String, Long>> distinctSourceIds = new DistinctSourceIds()
      .execute(graph)
      .map(c -> Tuple2.of("distinct.source.vertices", c))
      .returns(typeHint);

    //----------------------------------------------------------------------------------------------
    // Distinct Target Vertices
    //----------------------------------------------------------------------------------------------
    DataSet<Tuple2<String, Long>> distinctTargetIds = new DistinctTargetIds()
      .execute(graph)
      .map(c -> Tuple2.of("distinct.target.vertices", c))
      .returns(typeHint);

    //----------------------------------------------------------------------------------------------
    // Distinct Source Vertices by Edge Label
    //----------------------------------------------------------------------------------------------
    DataSet<Tuple2<String, Long>> sourceVerticesByEdgeLabel = new DistinctSourceIdsByEdgeLabel()
      .execute(graph)
      .map(withCount -> Tuple2.of(
        String.format("edges.%s.distinct.source.vertices", withCount.getObject()),
        withCount.getCount()))
      .returns(typeHint);

    //----------------------------------------------------------------------------------------------
    // Distinct Target Vertices by Edge Label
    //----------------------------------------------------------------------------------------------
    DataSet<Tuple2<String, Long>> targetVerticesByEdgeLabel = new DistinctTargetIdsByEdgeLabel()
      .execute(graph)
      .map(withCount -> Tuple2.of(
        String.format("edges.%s.distinct.target.vertices", withCount.getObject()),
        withCount.getCount()))
      .returns(typeHint);

    //----------------------------------------------------------------------------------------------
    // Distinct Edge Property Values by label - property name pair
    //----------------------------------------------------------------------------------------------
    DataSet<Tuple2<String, Long>> distinctEdgePropertyValuesByLabelAndPropertyName =
      new DistinctEdgePropertyValuesByLabelAndPropertyName()
      .execute(graph)
      .map(pair -> Tuple2.of(
        String.format("edges.%s.%s.distinct.property.values",
          pair.f0.f0, pair.f0.f1),
        pair.f1))
      .returns(typeHint);

    //----------------------------------------------------------------------------------------------
    // Distinct Vertex Property Values by label - property name pair
    //----------------------------------------------------------------------------------------------
    DataSet<Tuple2<String, Long>> distinctVertexPropertyValuesByLabelAndPropertyName =
      new DistinctVertexPropertyValuesByLabelAndPropertyName()
        .execute(graph)
        .map(pair -> Tuple2.of(
          String.format("vertex.%s.%s.distinct.property.values",
            pair.f0.f0, pair.f0.f1),
          pair.f1))
        .returns(typeHint);

    //----------------------------------------------------------------------------------------------
    // Distinct Edge Property Values by property name
    //----------------------------------------------------------------------------------------------
    DataSet<Tuple2<String, Long>> distinctEdgePropertyValuesByLabel =
      new DistinctEdgePropertyValuesByLabelAndPropertyName()
        .execute(graph)
        .map(pair -> Tuple2.of(
          String.format("edges.distinct.property.values", pair.f0),
          pair.f1))
        .returns(typeHint);

    //----------------------------------------------------------------------------------------------
    // Distinct Vertex Property Values by property name
    //----------------------------------------------------------------------------------------------
    DataSet<Tuple2<String, Long>> distinctVertexPropertyValuesByLabel =
      new DistinctVertexPropertyValuesByLabelAndPropertyName()
        .execute(graph)
        .map(pair -> Tuple2.of(
          String.format("vertex.%s.distinct.property.values", pair.f0),
          pair.f1))
        .returns(typeHint);

    vertexCount
      .union(edgeCount)
      .union(vertexLabelDistribution)
      .union(edgeLabelDistribution)
      .union(distinctSourceIds)
      .union(distinctTargetIds)
      .union(sourceVerticesByEdgeLabel)
      .union(targetVerticesByEdgeLabel)
      .union(distinctEdgePropertyValuesByLabelAndPropertyName)
      .union(distinctVertexPropertyValuesByLabelAndPropertyName)
      .union(distinctEdgePropertyValuesByLabel)
      .union(distinctVertexPropertyValuesByLabel)
      .sortPartition(0, Order.ASCENDING)
      .setParallelism(1)
      .writeAsCsv(outputFile, System.lineSeparator(), "=");

    getExecutionEnvironment().execute();
  }

  @Override
  public String getDescription() {
    return "Graph Statistics";
  }
}
