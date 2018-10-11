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
package org.gradoop.flink.datagen.transactions.foodbroker;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.codehaus.jettison.json.JSONException;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerEdgeLabels;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerVertexLabels;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.epgm.ByProperty;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class FoodBrokerTest extends GradoopFlinkTestBase {

  private GraphCollection cases;

  @Test
  public void testGenerate() throws Exception {
    generateCollection();

    assertNotNull(cases);
  }

  @Test
  public void testSalesQuotationLineCount() throws IOException, JSONException {
    generateCollection();
    DataSet<Vertex> salesQuotationLines = cases.getVertices()
      .filter(new ByLabel<>("SalesQuotationLine"));
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
  public void testSalesOrderCount() throws IOException, JSONException {
    generateCollection();
    DataSet<Vertex> salesQuotations = cases.getVertices()
      .filter(new ByLabel<>("SalesQuotation"));
    DataSet<Vertex> salesOrders = cases.getVertices()
      .filter(new ByLabel<>("SalesOrder"));

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
  public void testMaxVertexCount() throws IOException, JSONException {
    generateCollection();
    int casesCount = 10;
    double actual = 0;
    // max 1:SalesQuotation, 20:SalesQuotationLines, 1:SalesOrder,
    // 20:SalesOrderLines, 20:PurchOrders, 20:PurchOrderLines,
    // 20:DeliveryNotes, 20:PurchInvoices, 1:SalesInvoice = 63
    double max = 63 * casesCount;
    try {
      actual = cases.getVertices()
        .filter(new ByProperty<>("kind", PropertyValue.create("TransData")))
        .count();
    } catch (Exception e) {
      e.printStackTrace();
    }

    Assert.assertTrue(actual < max);
  }

  @Test
  public void testMaxEdgeCount() throws IOException, JSONException {
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

    Assert.assertTrue(actual < max);
  }

  @Test
  public void testLargeSetStatistics() throws Exception {

    String configPath = getFilePath("/foodbroker/config.json");

    FoodBrokerConfig config = FoodBrokerConfig.fromFile(configPath);

    config.setScaleFactor(1);

    FoodBroker foodBroker = new FoodBroker(getExecutionEnvironment(), getConfig(), config);

    List<GraphTransaction> result10K = foodBroker.execute().getGraphTransactions().collect();

    assertEquals(1000, result10K.size());

    for (GraphTransaction graph : result10K) {
      Set<GradoopId> vertexIds = Sets.newHashSetWithExpectedSize(graph.getVertices().size());

      for (Vertex vertex : graph.getVertices()) {
        vertexIds.add(vertex.getId());
        assertTrue(vertex.getGraphIds().size() >= 1);
      }

      // EDGE CONSISTENCY

      for (Edge edge : graph.getEdges()) {
        assertTrue("graph does not contain source of " + edge.getLabel(),
          vertexIds.contains(edge.getSourceId()));

        if (!vertexIds.contains(edge.getTargetId())) {
          System.out.println(graph.getVertexById(edge.getSourceId()));
        }

        assertTrue("graph does not contain target of " + edge.getLabel(),
          vertexIds.contains(edge.getTargetId()));
      }
    }
  }

  @Test
  public void testSchema() throws Exception {

    Set<String> foundVertexLabels = Sets.newHashSet();
    Set<String> foundEdgeLabels = Sets.newHashSet();


    for (int ignored : new int[] {0, 1, 2}) {
      generateCollection();

      for (Vertex vertex : cases.getVertices().collect()) {
        foundVertexLabels.add(vertex.getLabel());
      }

      for (Edge edge : cases.getEdges().collect()) {
        foundEdgeLabels.add(edge.getLabel());
      }
    }

    Set<String> expectedVertexLabels = Sets.newHashSet(
      FoodBrokerVertexLabels.CLIENT_VERTEX_LABEL,
      FoodBrokerVertexLabels.CUSTOMER_VERTEX_LABEL,
      FoodBrokerVertexLabels.DELIVERYNOTE_VERTEX_LABEL,
      FoodBrokerVertexLabels.EMPLOYEE_VERTEX_LABEL,
      FoodBrokerVertexLabels.LOGISTICS_VERTEX_LABEL,
      FoodBrokerVertexLabels.PRODUCT_VERTEX_LABEL,
      FoodBrokerVertexLabels.PURCHINVOICE_VERTEX_LABEL,
      FoodBrokerVertexLabels.SALESINVOICE_VERTEX_LABEL,
      FoodBrokerVertexLabels.PURCHORDER_VERTEX_LABEL,
      FoodBrokerVertexLabels.SALESORDER_VERTEX_LABEL,
      FoodBrokerVertexLabels.SALESQUOTATION_VERTEX_LABEL,
      FoodBrokerVertexLabels.TICKET_VERTEX_LABEL,
      FoodBrokerVertexLabels.USER_VERTEX_LABEL,
      FoodBrokerVertexLabels.VENDOR_VERTEX_LABEL
    );

    for (String label : expectedVertexLabels) {
      assertTrue( label + " vertices are missing", foundVertexLabels.contains(label));
    }

    for (String label : foundVertexLabels) {
      assertTrue( label + " vertices was not expected", expectedVertexLabels.contains(label));
    }

    Set<String> expectedEdgeLabels = Sets.newHashSet(
      FoodBrokerEdgeLabels.ALLOCATEDTO_EDGE_LABEL,
      FoodBrokerEdgeLabels.BASEDON_EDGE_LABEL,
      FoodBrokerEdgeLabels.CONCERNS_EDGE_LABEL,
      FoodBrokerEdgeLabels.CONTAINS_EDGE_LABEL,
      FoodBrokerEdgeLabels.CREATEDBY_EDGE_LABEL,
      FoodBrokerEdgeLabels.CREATEDFOR_EDGE_LABEL,
      FoodBrokerEdgeLabels.OPENEDBY_EDGE_LABEL,
      FoodBrokerEdgeLabels.OPERATEDBY_EDGE_LABEL,
      FoodBrokerEdgeLabels.PLACEDAT_EDGE_LABEL,
      FoodBrokerEdgeLabels.PROCESSEDBY_EDGE_LABEL,
      FoodBrokerEdgeLabels.PURCHORDERLINE_EDGE_LABEL,
      FoodBrokerEdgeLabels.RECEIVEDFROM_EDGE_LABEL,
      FoodBrokerEdgeLabels.SALESORDERLINE_EDGE_LABEL,
      FoodBrokerEdgeLabels.SALESQUOTATIONLINE_EDGE_LABEL,
      FoodBrokerEdgeLabels.SENTBY_EDGE_LABEL,
      FoodBrokerEdgeLabels.SENTTO_EDGE_LABEL,
      FoodBrokerEdgeLabels.SERVES_EDGE_LABEL
    );

    for (String label : expectedEdgeLabels) {
      assertTrue( label + " edges are missing", foundEdgeLabels.contains(label));
    }

    for (String label : foundEdgeLabels) {
      assertTrue( label + " edges was not expected", expectedEdgeLabels.contains(label));
    }
  }

  private void generateCollection() throws IOException, JSONException {

    if (cases == null) {
      String configPath = getFilePath("/foodbroker/config.json");

      FoodBrokerConfig config = FoodBrokerConfig.fromFile(configPath);

      config.setScaleFactor(0);

      FoodBroker foodBroker =
        new FoodBroker(getExecutionEnvironment(), getConfig(), config);

      cases = foodBroker.execute();
    }
  }


}