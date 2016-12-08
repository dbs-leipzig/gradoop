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

package org.gradoop.examples.biiig;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.hadoop.util.Time;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.flink.algorithms.fsm.transactional.GSpanIterative;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.ApplyAggregation;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.Sum;
import org.gradoop.flink.model.impl.operators.transformation.ApplyTransformation;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.math.BigDecimal;

import static java.math.BigDecimal.ROUND_HALF_UP;
import static java.math.BigDecimal.ZERO;

/**
 * Example workflow of paper "Scalable Business Intelligence with Graph
 * Collections" submitted to IT special issue on Big Data Analytics
 *
 * To execute the example:
 * 1. checkout Gradoop
 * 2. mvn clean package
 * 3. run main method
 */
public class FrequentLossPatterns
  extends AbstractRunner implements ProgramDescription {

  /**
   * Property key storing the vertex source identifier.
   */
  public static final String SOURCEID_KEY = "num";
  /**
   * Property key used to store the financial result.
   */
  public static final String RESULT_KEY = "financialResult";
  /**
   * Property key used to store the number of master data instances.
   */
  private static final String MASTERDATA_KEY = "masterDataCount";
  /**
   * Label prefix of master data
   */
  private static final String MASTER_PREFIX = "M#";
  /**
   * Label prefix of transactional data.
   */
  private static final String TRANSACTIONAL_PREFIX = "T#";

  // TRANSFORMATION FUNCTIONS

  /**
   * Relabel vertices and to drop properties.
   *
   * @param current current vertex
   * @param transformed copy of current except label and properties
   * @return current vertex with a new label depending on its type
   */
  private static Vertex relabelVerticesAndRemoveProperties(Vertex current,
    Vertex transformed) {

    String label;

    if (current
      .getPropertyValue(BusinessTransactionGraphs.SUPERTYPE_KEY).getString()
      .equals(BusinessTransactionGraphs.SUPERCLASS_VALUE_TRANSACTIONAL)) {

      label = TRANSACTIONAL_PREFIX + current.getLabel();
    } else {
      label = MASTER_PREFIX + current.getPropertyValue(SOURCEID_KEY).toString();
    }

    transformed.setLabel(label);

    return transformed;
  }

  /**
   * Drop edge properties.
   *
   * @param current current edge
   * @param transformed copy of current except label and properties
   * @return current edge without properties
   */
  private static Edge dropEdgeProperties(Edge current, Edge transformed) {
    transformed.setLabel(current.getLabel());
    return transformed;
  }

  /**
   * Append graph label by FSM support.
   *
   * @param current current graph head
   * @param transformed copy of current except label and properties
   * @return graph head with additional support property
   */
  private static GraphHead addSupportToGraphHead(GraphHead current,
    GraphHead transformed) {

    BigDecimal support = current
      .getPropertyValue(TFSMConstants.FREQUENCY_KEY)
      .getBigDecimal().setScale(2, ROUND_HALF_UP);

    String newLabel = current.getLabel() + " (" + support + ")";

    transformed.setLabel(newLabel);
    transformed.setProperties(null);

    return transformed;
  }

  /**
   * main method
   * @param args arguments (none required)
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    // avoids multiple output files
    getExecutionEnvironment().setParallelism(1);
    // get Gradoop configuration
    GradoopFlinkConfig config = GradoopFlinkConfig
      .createConfig(getExecutionEnvironment());

    // START DEMONSTRATION PROGRAM

    // (1) read data from source

    String graphHeadPath = FrequentLossPatterns.class
      .getResource("/data/json/foodbroker/graphs.json").getFile();

    String vertexPath = FrequentLossPatterns.class.
      getResource("/data/json/foodbroker/nodes.json").getFile();

    String edgePath = FrequentLossPatterns.class
      .getResource("/data/json/foodbroker/edges.json").getFile();

    JSONDataSource dataSource = new JSONDataSource(
      graphHeadPath, vertexPath, edgePath, config);

    LogicalGraph iig = dataSource.getLogicalGraph();

    // (2) extract collection of business transaction graphs

    GraphCollection btgs = iig
      .callForCollection(new BusinessTransactionGraphs());

    // (3) aggregate financial result
    btgs = btgs.apply(new ApplyAggregation(new Result()));

    // (4) select by loss (negative financialResult)
    btgs = btgs.select(
      g -> g.getPropertyValue(RESULT_KEY).getBigDecimal().compareTo(ZERO) < 0);

    // (5) relabel vertices and remove vertex and edge properties

    btgs = btgs.apply(new ApplyTransformation(
      TransformationFunction.keep(),
      FrequentLossPatterns::relabelVerticesAndRemoveProperties,
      FrequentLossPatterns::dropEdgeProperties
    ));

    // (6) mine frequent subgraphs

    FSMConfig fsmConfig = new FSMConfig(0.6f, true);

    GraphCollection frequentSubgraphs = btgs
      .callForCollection(new GSpanIterative(fsmConfig));

    // (7) Check, if frequent subgraph contains master data

    frequentSubgraphs = frequentSubgraphs.apply(
      new ApplyAggregation(new DetermineMasterDataSurplus()));

    // (8) Select graphs containing master data

    frequentSubgraphs = frequentSubgraphs.select(
      g -> g.getPropertyValue(MASTERDATA_KEY).getInt() >= 0);

    // (9) relabel graph heads of frequent subgraphs

    frequentSubgraphs = frequentSubgraphs.apply(new ApplyTransformation(
      FrequentLossPatterns::addSupportToGraphHead,
      TransformationFunction.keep(),
      TransformationFunction.keep()
    ));

    // (10) write data sink

    String outPath =
      System.getProperty("user.home") + "/lossPatterns_" + Time.now() + ".dot";

    new DOTDataSink(outPath, true).write(frequentSubgraphs);

    // END DEMONSTRATION PROGRAM

    // trigger execution
    getExecutionEnvironment().execute();
  }

  // AGGREGATE FUNCTIONS

  /**
   * Calculate the financial result of business transaction graphs.
   */
  private static class Result
    extends Sum implements VertexAggregateFunction {

    /**
     * Property key for revenue values.
     */
    private static final String REVENUE_KEY = "revenue";
    /**
     * Property key for expense values.
     */
    private static final String EXPENSE_KEY = "expense";


    @Override
    public PropertyValue getVertexIncrement(Vertex vertex) {
      PropertyValue increment;

      if (vertex.hasProperty(REVENUE_KEY)) {
        increment = vertex.getPropertyValue(REVENUE_KEY);

      } else if (vertex.hasProperty(EXPENSE_KEY)) {
        PropertyValue expense = vertex.getPropertyValue(EXPENSE_KEY);
        increment = PropertyValueUtils.Numeric
          .multiply(expense, PropertyValue.create(-1));

      } else {
        increment = PropertyValue.create(0);
      }

      return increment;
    }

    @Override
    public String getAggregatePropertyKey() {
      return RESULT_KEY;
    }
  }

  /**
   * Counts master data vertices less than the number of transactional vertices.
   */
  private static class DetermineMasterDataSurplus
    extends Sum implements VertexAggregateFunction {

    @Override
    public PropertyValue getVertexIncrement(Vertex vertex) {
      return vertex.getLabel().startsWith(MASTER_PREFIX) ?
        PropertyValue.create(1) : PropertyValue.create(-1);
    }

    @Override
    public String getAggregatePropertyKey() {
      return MASTERDATA_KEY;
    }
  }


  @Override
  public String getDescription() {
    return  FrequentLossPatterns.class.getName();
  }
}
