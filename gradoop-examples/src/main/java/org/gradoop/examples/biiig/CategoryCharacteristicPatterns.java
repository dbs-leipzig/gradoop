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

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.examples.utils.ExampleOutput;
import org.gradoop.flink.algorithms.fsm.ccs.CategoryCharacteristicSubgraphs;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.flink.model.impl.operators.aggregation.ApplyAggregation;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.bool.Or;
import org.gradoop.flink.model.impl.operators.transformation.ApplyTransformation;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Example workflow of paper "Scalable Business Intelligence with Graph
 * Collections" submitted to IT special issue on Big Data Analytics
 *
 * To execute the example:
 * 1. checkout Gradoop
 * 2. mvn clean install
 * 3. run main method
 */
public class CategoryCharacteristicPatterns implements ProgramDescription {

  /**
   * main method
   * @param args arguments (none required)
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    ExampleOutput out = new ExampleOutput();

    LogicalGraph iig = getIntegratedInstanceGraph();

    out.add("Integrated Instance Graph", iig);

    GraphCollection btgs = iig
      .callForCollection(new BusinessTransactionGraphs());

    btgs = btgs
      .apply(new ApplyAggregation(new IsClosedAggregateFunction()));

    btgs = btgs.select(new IsClosedPredicateFunction());

    btgs = btgs
      .apply(new ApplyAggregation(new CountSalesOrdersAggregateFunction()));

    out.add("Business Transaction Graphs with Measures", btgs);

    btgs = btgs.apply(new ApplyTransformation(
      new CategorizeGraphsTransformationFunction(),
      new RelabelVerticesTransformationFunction(),
      new EdgeLabelOnlyTransformationFunction())
    );

    out.add("Business Transaction Graphs after Transformation", btgs);

    FSMConfig fsmConfig = new FSMConfig(0.8f, true, 1, 3);

    GraphCollection patterns = btgs
      .callForCollection(new CategoryCharacteristicSubgraphs(fsmConfig, 2.0f));

    out.add("Category characteristic graph patters", patterns);

    out.print();
  }

  /**
   * Returns example integrated instance graph from GDL input.
   * @return integrated instance graph
   * @throws IOException
   */
  public static LogicalGraph getIntegratedInstanceGraph() throws IOException {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    GradoopFlinkConfig gradoopConf = GradoopFlinkConfig.createConfig(env);

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(gradoopConf);

    String gdl = IOUtils.toString(CategoryCharacteristicPatterns.class
      .getResourceAsStream("/data/gdl/itbda.gdl"));

    gdl = gdl
      .replaceAll("SOURCEID_KEY",
        BusinessTransactionGraphs.SOURCEID_KEY)
      .replaceAll("SUPERTYPE_KEY",
        BusinessTransactionGraphs.SUPERTYPE_KEY)
      .replaceAll("SUPERCLASS_VALUE_MASTER",
        BusinessTransactionGraphs.SUPERCLASS_VALUE_MASTER)
      .replaceAll("SUPERCLASS_VALUE_TRANSACTIONAL",
        BusinessTransactionGraphs.SUPERCLASS_VALUE_TRANSACTIONAL);

    loader.initDatabaseFromString(gdl);

    return loader.getLogicalGraphByVariable("iig");
  }

  /**
   * Aggregate function to determine "isClosed" measure
   */
  private static class IsClosedAggregateFunction extends Or
    implements VertexAggregateFunction {

    @Override
    public String getAggregatePropertyKey() {
      return "isClosed";
    }

    @Override
    public PropertyValue getVertexIncrement(Vertex vertex) {

      boolean isClosedQuotation =
        vertex.getLabel().equals("Quotation") &&
          !vertex.getPropertyValue("status").toString().equals("open");

      return PropertyValue.create(isClosedQuotation);
    }
  }

  /**
   * Predicate function to filter graphs by "isClosed" == true
   */
  private static class IsClosedPredicateFunction
    implements FilterFunction<GraphHead> {

    @Override
    public boolean filter(GraphHead graphHead) throws Exception {
      return graphHead.getPropertyValue("isClosed").getBoolean();
    }
  }

  /**
   * Aggregate function to count sales orders per graph.
   */
  private static class CountSalesOrdersAggregateFunction
    extends Count implements VertexAggregateFunction {

    @Override
    public PropertyValue getVertexIncrement(Vertex vertex) {
      return PropertyValue.create(
        vertex.getLabel().equals("SalesOrder") ? 1 : 0);
    }

    @Override
    public String getAggregatePropertyKey() {
      return "soCount";
    }
  }

  /**
   * Transformation function to categorize graphs.
   */
  private static class CategorizeGraphsTransformationFunction implements
    TransformationFunction<GraphHead> {
    @Override
    public GraphHead execute(GraphHead current,
      GraphHead transformed) {

      String category =
        current.getPropertyValue("soCount").getInt() > 0 ? "won" : "lost";

      transformed.setProperty(
        CategoryCharacteristicSubgraphs.CATEGORY_KEY,
        PropertyValue.create(category)
      );

      return transformed;
    }
  }

  /**
   * Transformation function to relabel vertices and to drop properties.
   */
  private static class RelabelVerticesTransformationFunction implements
    TransformationFunction<Vertex> {
    @Override
    public Vertex execute(Vertex current, Vertex transformed) {

      transformed.setLabel(current.getPropertyValue(
        BusinessTransactionGraphs.SUPERTYPE_KEY).toString()
        .equals(BusinessTransactionGraphs.SUPERCLASS_VALUE_TRANSACTIONAL) ?
        current.getLabel() :
        current
          .getPropertyValue(BusinessTransactionGraphs.SOURCEID_KEY).toString()
      );

      return transformed;
    }
  }

  /**
   * Transformation function to drop properties of edges.
   */
  private static class EdgeLabelOnlyTransformationFunction implements
    TransformationFunction<Edge> {
    @Override
    public Edge execute(Edge current, Edge transformed) {

      transformed.setLabel(current.getLabel());

      return transformed;
    }
  }

  @Override
  public String getDescription() {
    return  CategoryCharacteristicPatterns.class.getName();
  }
}
