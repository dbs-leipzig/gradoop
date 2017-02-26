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

package org.gradoop.flink.datagen.foodbroker;

import org.apache.flink.api.java.DataSet;
import org.codehaus.jettison.json.JSONException;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.epgm.ByProperty;
import org.gradoop.flink.model.impl.functions.epgm.Label;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FoodBrokerTest extends GradoopFlinkTestBase {

  @Test
  public void testGenerate() throws Exception {
    GraphCollection cases = generateCollection();

    assertNotNull(cases);
  }

  @Test
  public void testSalesQuotationLineCount() throws IOException, JSONException, URISyntaxException {
    GraphCollection cases = generateCollection();
    DataSet<Vertex> salesQuotationLines = cases.getVertices()
      .filter(new ByLabel<Vertex>("SalesQuotationLine"));
    int min = 1;
    int max = 20;
    int casesCount = 10;
    int epsilon = (max-min)*casesCount;
    long count = 0;
    long expected = ((min + max) / 2) * casesCount;

    try {
      count = salesQuotationLines.count();
    } catch (Exception e) {
      e.printStackTrace();
    }
    assertEquals(expected, count, epsilon);
  }

  @Test
  public void testSalesOrderCount() throws IOException, JSONException, URISyntaxException {
    GraphCollection cases = generateCollection();
    DataSet<Vertex> salesQuotations = cases.getVertices()
      .filter(new ByLabel<Vertex>("SalesQuotation"));
    DataSet<Vertex> salesOrders = cases.getVertices()
      .filter(new ByLabel<Vertex>("SalesOrder"));

    double actual = 0;
    double expected = 0;
    double confirmationProbability = 0.6;
    double epsilon = 0;
    try {
      expected = salesQuotations.count() * confirmationProbability;
      actual = salesOrders.count();
      //influence is +0.2 for each master data object -> 0.4
      //so epsilon is (quotation count * 0.6+0.4) minus
      //(quotation count * 0.6)
      epsilon = salesQuotations.count() - expected;
    } catch (Exception e) {
      e.printStackTrace();
    }
    //2times 0.2, once for each influencing master data object
    assertEquals(expected, actual, epsilon);
  }

  @Test
  public void testMaxVertexCount() throws IOException, JSONException, URISyntaxException {
    GraphCollection cases = generateCollection();
    int casesCount = 10;
    double actual = 0;
    // max 1:SalesQuotation, 20:SalesQuotationLines, 1:SalesOrder,
    // 20:SalesOrderLines, 20:PurchOrders, 20:PurchOrderLines,
    // 20:DeliveryNotes, 20:PurchInvoices, 1:SalesInvoice = 63
    double max = 63 * casesCount;
    try {
      actual = cases.getVertices()
        .filter(new ByProperty<Vertex>("kind", PropertyValue.create("TransData")))
        .count();
    } catch (Exception e) {
      e.printStackTrace();
    }

    Assert.assertTrue(actual < max);
  }

  @Test
  public void testMaxEdgeCount() throws IOException, JSONException, URISyntaxException {
    GraphCollection cases = generateCollection();
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

  private GraphCollection generateCollection()
    throws IOException, JSONException, URISyntaxException {
    String configPath = Paths.get(
      FoodBrokerTest.class.getResource("/foodbroker/config.json").toURI()).toFile().getPath();

    FoodBrokerConfig config = FoodBrokerConfig.fromFile(configPath);

    config.setScaleFactor(0);

    FoodBroker foodBroker =
      new FoodBroker(getExecutionEnvironment(), getConfig(), config);

    try {
      return foodBroker.execute();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

}