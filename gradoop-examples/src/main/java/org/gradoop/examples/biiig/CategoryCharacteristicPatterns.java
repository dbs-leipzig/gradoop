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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.utils.ExampleOutput;
import org.gradoop.flink.algorithms.fsm.ccs.CategoryCharacteristicSubgraphs;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.flink.model.impl.operators.aggregation.ApplyAggregation;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.bool.Or;
import org.gradoop.flink.model.impl.operators.transformation.ApplyTransformation;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

import static org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs.SOURCEID_KEY;
import static org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs.SUPERCLASS_VALUE_MASTER;
import static org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs.SUPERCLASS_VALUE_TRANSACTIONAL;
import static org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs.SUPERTYPE_KEY;
import static org.gradoop.flink.algorithms.fsm.ccs.CategoryCharacteristicSubgraphs.CATEGORY_KEY;

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

    // extract business transaction graphs (BTGs)
    GraphCollection btgs = iig
      .callForCollection(new BusinessTransactionGraphs());

    btgs = btgs
      // determine closed/open BTGs
      .apply(new ApplyAggregation(new IsClosedAggregateFunction()))
      // select closed BTGs
      .select(g -> g.getPropertyValue("isClosed").getBoolean())
      // count number of sales orders per BTG
      .apply(new ApplyAggregation(new CountSalesOrdersAggregateFunction()));

    out.add("Business Transaction Graphs with Measures", btgs);

    btgs = btgs.apply(new ApplyTransformation(
      // Transformation function to categorize graphs
      (graph, copy) -> {
        String category = graph.getPropertyValue("soCount").getInt() > 0 ?
          "won" : "lost";
        copy.setProperty(CATEGORY_KEY, PropertyValue.create(category));
        return copy;
      },
      // Transformation function to relabel vertices and to drop properties
      (vertex, copy) -> {
        String superType = vertex.getPropertyValue(SUPERTYPE_KEY).toString();

        if (superType.equals(SUPERCLASS_VALUE_TRANSACTIONAL)) {
          copy.setLabel(vertex.getLabel());
        } else { // master data
          copy.setLabel(vertex.getPropertyValue(SOURCEID_KEY).toString());
        }
        return copy;
      },
      // Transformation function to drop properties of edges
      (edge, copy) -> {
        copy.setLabel(edge.getLabel());

        return copy;
      })
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
        SOURCEID_KEY)
      .replaceAll("SUPERTYPE_KEY",
        SUPERTYPE_KEY)
      .replaceAll("SUPERCLASS_VALUE_MASTER",
        SUPERCLASS_VALUE_MASTER)
      .replaceAll("SUPERCLASS_VALUE_TRANSACTIONAL",
        SUPERCLASS_VALUE_TRANSACTIONAL);

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

  @Override
  public String getDescription() {
    return  CategoryCharacteristicPatterns.class.getName();
  }
}
