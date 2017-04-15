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
package org.gradoop.benchmark.nesting;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.benchmark.nesting.serializers.Bogus;
import org.gradoop.benchmark.nesting.serializers.DeserializeGradoopidFromFile;
import org.gradoop.benchmark.nesting.serializers.DeserializePairOfIdsFromFile;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.nest.NestingBase;
import org.gradoop.flink.model.impl.operators.nest.model.NestedModel;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingResult;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Class implementing the serialization methods
 */
public class PerformBenchmarkOverSerializedData extends AbstractBenchmark {

  /**
   * Indices for the left operand
   */
  private NestingIndex leftOperand;

  /**
   * Indices for the right operand
   */
  private NestingIndex rightOperand;

  /**
   * Defines the data model where the operations are performed
   */
  private NestedModel model;

  /**
   * Default constructor for running the tests
   * @param basePath    Path where to obtain all the data
   */
  public PerformBenchmarkOverSerializedData(String basePath, String benchmarkPath) {
    super(basePath, benchmarkPath);
  }

  /**
   * Main program entrance
   * @param args        System arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    runBenchmark(PerformBenchmarkOverSerializedData.class, args);
  }

  @Override
  public void loadOperands() {
    String path = getBasePath();
    leftOperand = loadNestingIndex(generateOperandBasePath(path, true));
    rightOperand = loadNestingIndex(generateOperandBasePath(path, false));
    NestingIndex nestedRepresentation = NestingBase.mergeIndices(leftOperand, rightOperand);
    LogicalGraph flat = readLogicalGraph(path);
    model = new NestedModel(flat, nestedRepresentation);
  }

  @Override
  public void performOperation() {
    NestingBase.nest(model, leftOperand, rightOperand, GradoopId.get());
    model.disjunctiveSemantics(model.getPreviousResult(), rightOperand);
  }

  @Override
  public void finalizeLoadOperand() {
    // Counting each element for the loaded index, alongside with the values of the flattened
    // graph
    indexCount(leftOperand);
    indexCount(rightOperand);
    model.getFlattenedGraph().getGraphHead().output(new Bogus<>());
    model.getFlattenedGraph().getVertices().output(new Bogus<>());
    model.getFlattenedGraph().getEdges().output(new Bogus<>());
  }

  @Override
  public void finalizeOperation() {
    // Counting the computation actually required to produce the result, that is the graph stack
    // Alongside with the resulting indices
    NestingResult result = model.getPreviousResult();
    LogicalGraph counter = NestingBase.toLogicalGraph(result, model.getFlattenedGraph());
    counter.getGraphHead().output(new Bogus<>());
    counter.getVertices().output(new Bogus<>());
    counter.getEdges().output(new Bogus<>());
  }

  /**
   * Counting the indices for benchmarking. No other operation beside consuming data is done
   * @param index   Index containing the data
   */
  private void indexCount(NestingIndex index) {
    index.getGraphHeads().output(new Bogus<>());
    index.getGraphEdgeMap().output(new Bogus<>());
    index.getGraphVertexMap().output(new Bogus<>());
  }

}
