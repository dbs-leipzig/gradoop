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
package org.gradoop.model.impl.datagen.foodbroker;

import org.apache.flink.api.java.DataSet;
import org.codehaus.jettison.json.JSONException;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GradoopFlinkTestUtils;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.functions.LabledVerticesLineFromVertices;

import org.gradoop.model.impl.datagen.foodbroker.functions
  .TransactionalVerticesFromVertices;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class FoodBrokerTest extends GradoopFlinkTestBase {
  @Test
  public void testGenerate() throws Exception {

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> cases =
      generateCollection();

    GradoopFlinkTestUtils.printGraphCollection(cases);
  }

  @Test
  public void testSalesQuotationLineCount() throws IOException, JSONException {
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> cases =
      generateCollection();
    DataSet<VertexPojo> salesQuotationLine = cases.getVertices()
      .filter(
        new LabledVerticesLineFromVertices<VertexPojo>("SalesQuotationLine"));
    int min = 1;
    int max = 20;
    int casesCount = 10;
    int epsilon = (max-min)*casesCount;
    long count = 0;
    long expected = ((min + max) / 2) * casesCount;

    try {
      count = salesQuotationLine.count();
    } catch (Exception e) {
      e.printStackTrace();
    }
    assertEquals(expected, count, epsilon);
  }

  @Test
  public void testSalesOrderCount() throws IOException, JSONException {
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> cases =
      generateCollection();
    DataSet<VertexPojo> salesQuotation = cases.getVertices()
      .filter(
        new LabledVerticesLineFromVertices<VertexPojo>("SalesQuotation"));
    DataSet<VertexPojo> salesOrder = cases.getVertices()
      .filter(
        new LabledVerticesLineFromVertices<VertexPojo>("SalesOrder"));

    double actual = 0;
    double expected = 0;
    double confirmationProbability = 0.6;
    double epsilon = 0;
    try {
      expected = salesQuotation.count() * confirmationProbability;
      actual = salesOrder.count();
      //influence is +0.2 for each master data object -> 0.4
      //so epsilon is (quotation count * 0.6+0.4) minus
      //(quotation count * 0.6)
      epsilon = salesQuotation.count() - expected;
    } catch (Exception e) {
      e.printStackTrace();
    }
    //2times 0.2, once for each influencing master data object
    assertEquals(expected, actual, epsilon);
  }

  @Test
  public void testMaxVertexCount() throws IOException, JSONException {
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> cases =
      generateCollection();
    int casesCount = 10;
    double actual = 0;
    //max 1:SalesQuotatio, 20:SalesQuotationLines, 1:SalesOrder,
    // 20:SalesOrderLines, 20:PurchOrders, 20:PurchOrderLines,
    // 20:DeliveryNotes, 20:PurchInvoices, 1:SalesInvoice = 63
    double max = 63 * casesCount;
    try {
      actual = cases.getVertices()
        .filter(new TransactionalVerticesFromVertices<VertexPojo>())
        .count();
    } catch (Exception e) {
      e.printStackTrace();
    }

    Assert.assertTrue(actual < max);
  }

  @Test
  public void testMaxEdgeCount() throws IOException, JSONException {
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> cases =
      generateCollection();
    int casesCount = 10;
    double actual = 0;
    double max = 1*2 + 20*2 + 1*3 + 20*2 + 20*3 + 20*2 + 20*2 + 20*1 + 1*1 ;
    max = max * casesCount;
    try {
      actual = cases.getEdges()
        .count();
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println(actual + "  " + max);

    Assert.assertTrue(actual < max);
  }

  private GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> generateCollection() throws
    IOException, JSONException {
    FoodBrokerConfig config = FoodBrokerConfig.fromFile(
      FoodBrokerTest.class.getResource("/foodbroker/config.json").getFile());

    config.setScaleFactor(0);

    FoodBroker<GraphHeadPojo, VertexPojo, EdgePojo> foodBroker =
      new FoodBroker<>(getExecutionEnvironment(), getConfig(), config);

    return foodBroker.execute();
  }
}