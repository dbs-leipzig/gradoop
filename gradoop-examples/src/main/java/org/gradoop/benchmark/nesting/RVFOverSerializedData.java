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
  private LogicalGraph result;

  /**
   * Default constructor for running the tests
   * @param basePath    Path where to obtain all the data
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
  public void loadOperands() {
    String path = getBasePath();
    LogicalGraph flat = readLogicalGraph(path);
    leftOperand = NestingBase.toLogicalGraph(loadNestingIndex(generateOperandBasePath(path,
      true)), flat);
    rightOperand = NestingBase.toGraphCollection(loadNestingIndex(generateOperandBasePath(path,
      false)), flat);
    model = new ReduceVertexFusion();
  }

  @Override
  public void performOperation() {
    result = model.execute(leftOperand, rightOperand);
  }

  @Override
  public void finalizeLoadOperand() {
    // Counting each element for the loaded index, alongside with the values of the flattened
    // graph
    leftOperand.getGraphHead().output(new Bogus<>());
    leftOperand.getVertices().output(new Bogus<>());
    leftOperand.getEdges().output(new Bogus<>());
    rightOperand.getGraphHeads().output(new Bogus<>());
    rightOperand.getVertices().output(new Bogus<>());
    rightOperand.getEdges().output(new Bogus<>());
  }

  @Override
  public void finalizeOperation() {
    // Counting the computation actually required to produce the result, that is the graph stack
    // Alongside with the resulting indices
    result.getGraphHead().output(new Bogus<>());
    result.getVertices().output(new Bogus<>());
    result.getEdges().output(new Bogus<>());
  }
}
