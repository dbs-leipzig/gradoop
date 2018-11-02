/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.examples.biiig;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.flink.algorithms.fsm.TransactionalFSM;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.ApplyAggregation;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.Sum;
import org.gradoop.flink.model.impl.operators.transformation.ApplyTransformation;

import java.math.BigDecimal;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import static java.math.BigDecimal.ROUND_HALF_UP;
import static java.math.BigDecimal.ZERO;

/**
 * Example workflow of paper "Scalable Business Intelligence with Graph
 * Collections" submitted to IT special issue on Big Data Analytics
 *
 * To execute the example:
 * 1. install Graphviz (e.g., sudo apt-get install graphviz)
 *    See {@link <a href="http://www.graphviz.org/">Graphviz</a>}
 * 2. checkout Gradoop
 * 3. mvn clean package
 * 4. run main method
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
      .getPropertyValue(DIMSpanConstants.SUPPORT_KEY)
      .getBigDecimal().setScale(2, ROUND_HALF_UP);

    String newLabel = current.getLabel() + " (" + support + ")";

    transformed.setLabel(newLabel);
    transformed.setProperties(null);

    return transformed;
  }

  /**
   * main method
   * @param args arguments (none required)
   * @throws Exception on failure
   */
  public static void main(String[] args) throws Exception {

    // avoids multiple output files
    getExecutionEnvironment().setParallelism(1);

    // START DEMONSTRATION PROGRAM

    // (1) read data from source

    String csvPath = URLDecoder.decode(
      FrequentLossPatterns.class.getResource("/data/csv/foodbroker").getFile(),
      StandardCharsets.UTF_8.name());

    LogicalGraph iig = readLogicalGraph(csvPath);

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

    GraphCollection frequentSubgraphs = btgs
      .callForCollection(new TransactionalFSM(0.55f));

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

    String pathPrefix = System.getProperty("user.home") + "/lossPatterns";
    String dotPath = pathPrefix + ".dot";
    String psPath = pathPrefix + ".ps";

    new DOTDataSink(dotPath, true).write(frequentSubgraphs, true);

    // END DEMONSTRATION PROGRAM

    // trigger execution
    getExecutionEnvironment().execute();

    new ProcessBuilder("dot", "-Tps", dotPath, "-o", psPath).inheritIO().start();
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
