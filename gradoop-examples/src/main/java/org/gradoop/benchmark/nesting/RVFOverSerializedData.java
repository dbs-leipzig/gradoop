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

import org.gradoop.benchmark.nesting.data.AbstractBenchmark;
import org.gradoop.flink.model.impl.functions.utils.Self;
import org.gradoop.flink.model.impl.operators.nest.ReduceVertexFusion;
import org.gradoop.benchmark.nesting.serializers.Bogus;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.nest.NestingBase;

/**
 * Class implementing the serialization methods
 */
public class RVFOverSerializedData extends AbstractBenchmark {

  /**
   * Indices for the left operand
   */
  private LogicalGraph leftOperand;

  /**
   * Indices for the right operand
   */
  private GraphCollection rightOperand;

  /**
   * Defines the data model where the operations are performed
   */
  private ReduceVertexFusion model;

  /**
   * Storing the final result
   */
  private LogicalGraph result;

  /**
   * Default constructor for running the tests
   *
   * @param basePath        Base path where the indexed data is loaded
   * @param benchmarkPath   File where to store the intermediate results
   */
  public RVFOverSerializedData(String basePath, String benchmarkPath) {
    super(basePath, benchmarkPath);
  }

  /**
   * Main program entrance
   * @param args        System arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    runBenchmark(RVFOverSerializedData.class, args);
  }

  @Override
  public void performOperation() {
    String path = getBasePath();
    LogicalGraph flat = readLogicalGraph(path);
    leftOperand = NestingBase.toLogicalGraph(loadNestingIndex(generateOperandBasePath(path,
      true)), flat);
    rightOperand = NestingBase.toGraphCollection(loadNestingIndex(generateOperandBasePath(path,
      false)), flat);
    model = new ReduceVertexFusion();
    result = model.execute(leftOperand, rightOperand);
  }

  @Override
  public void benchmarkOperation() throws Exception {
    // Counting the computation actually required to produce the result, that is the graph stack
    // Alongside with the resulting indices
    registerLogicalGraph(result);
  }
}
