/**
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
package org.gradoop.flink.datagen.transactions.foodbroker.functions.process;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConstants;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Returns transactional data created in a complaint handling process together with new created
 * master data (user, clients).
 */
public class ComplaintHandling extends AbstractProcess
  implements MapFunction<GraphTransaction, GraphTransaction> {

  /**
   * Map wich stores the vertex for each gradoop id.
   */
  private Map<GradoopId, Vertex> masterDataMap;
  /**
   * Set containing all sales order lines for one graph transaction.
   */
  private Set<Edge> salesOrderLines;
  /**
   * Set containing all purch order lines for one graph transaction.
   */
  private Set<Edge> purchOrderLines;
  /**
   * The sales order from one graph transaction.
   */
  private Vertex salesOrder;
  private GraphTransaction graph;

  /**
   * Valued constructor.
   *
   * @param epgmGraphHeadFactory EPGM graph head factory
   * @param epgmVertexFactory EPGM vertex factory
   * @param epgmEdgeFactory EPGM edge factory
   * @param config FoodBroker configuration
   */
  public ComplaintHandling(EPGMGraphHeadFactory<GraphHead> epgmGraphHeadFactory,
    EPGMVertexFactory<Vertex> epgmVertexFactory, EPGMEdgeFactory<Edge> epgmEdgeFactory,
    FoodBrokerConfig config) {
    super(epgmGraphHeadFactory, epgmVertexFactory, epgmEdgeFactory, config);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public GraphTransaction map(GraphTransaction graph) throws Exception {
    this.graph = graph;

    //init new maps
    vertexMap = Maps.newHashMap();
    masterDataMap = Maps.newHashMap();
    userMap = Maps.newHashMap();
    graphIds = GradoopIdSet.fromExisting(graph.getGraphHead().getId());

    boolean confirmed = false;

    for (Vertex vertex : graph.getVertices()) {
      if (vertex.getLabel().equals(FoodBrokerConstants.SALESORDER_VERTEX_LABEL)) {
        confirmed = true;
        break;
      }
    }

    if (confirmed) {
      edgeMap = createEdgeMap(graph);
      //get needed transactional objects created during brokerage process
      Set<Vertex> deliveryNotes = getVertexByLabel(graph, FoodBrokerConstants.DELIVERYNOTE_VERTEX_LABEL);
      salesOrderLines = getEdgesByLabel(graph, FoodBrokerConstants.SALESORDERLINE_EDGE_LABEL);
      purchOrderLines = getEdgesByLabel(graph, FoodBrokerConstants.PURCHORDERLINE_EDGE_LABEL);
      salesOrder = getVertexByLabel(graph, FoodBrokerConstants.SALESORDER_VERTEX_LABEL).iterator().next();

      //the complaint handling process
      badQuality(deliveryNotes);
      lateDelivery(deliveryNotes);
      //get all created vertices and edges
      Set<Vertex> transactionalVertices = getVertices();
      Set<Edge> transactionalEdges = getEdges();
      //if one or more tickets were created
      if ((transactionalVertices.size() > 0) && (transactionalEdges.size() > 0)) {
        graph.getVertices().addAll(transactionalVertices);
        graph.getEdges().addAll(transactionalEdges);
        globalSeed++;
      }
    }

    for (Edge edge : graph.getEdges()) {
      if (edge.getGraphIds() == null) {
        System.out.println(edge);
      }
    }

    return graph;
  }

  /**
   * Creates a ticket if bad quality occurs.
   *
   * @param deliveryNotes all deliverynotes from the brokerage process
   */
  private void badQuality(Set<Vertex> deliveryNotes) {
    GradoopId purchOrderId;
    List<Float> influencingMasterQuality;
    Set<Edge> currentPurchOrderLines;
    Set<Edge> badSalesOrderLines;

    for (Vertex deliveryNote : deliveryNotes) {
      influencingMasterQuality = Lists.newArrayList();
      badSalesOrderLines = Sets.newHashSet();
      //get the corresponding purch order and purch order lines
      purchOrderId = getEdgeTargetId(FoodBrokerConstants.CONTAINS_EDGE_LABEL, deliveryNote.getId());
      currentPurchOrderLines = getPurchOrderLinesByPurchOrder(purchOrderId);

      for (Edge purchOrderLine : currentPurchOrderLines) {
        influencingMasterQuality.add(getQuality(productIndex, purchOrderLine.getTargetId()));
      }
      int containedProducts = influencingMasterQuality.size();
      // increase relative influence of vendor and logistics
      for (int i = 1; i <= containedProducts / 2; i++) {
        influencingMasterQuality.add(getEdgeTargetQuality(
          FoodBrokerConstants.OPERATEDBY_EDGE_LABEL, deliveryNote.getId(), FoodBrokerConstants.BC_LOGISTICS));
        influencingMasterQuality.add(getEdgeTargetQuality(FoodBrokerConstants.PLACEDAT_EDGE_LABEL,
          purchOrderId, FoodBrokerConstants.BC_VENDORS));
      }
      if (config.happensTransitionConfiguration(
        influencingMasterQuality, FoodBrokerConstants.TICKET_VERTEX_LABEL,
        FoodBrokerConstants.TI_BADQUALITYPROBABILITY_CONFIG_KEY, false)) {

        for (Edge purchOrderLine : currentPurchOrderLines) {
          badSalesOrderLines.add(getCorrespondingSalesOrderLine(purchOrderLine.getId()));
        }

        Vertex ticket = newTicket(
          FoodBrokerConstants.BADQUALITY_TICKET_PROBLEM,
          deliveryNote.getPropertyValue(FoodBrokerConstants.DATE_KEY).getLong());

        graph.getVertices().add(ticket);

        grantSalesRefund(badSalesOrderLines, ticket);
        claimPurchRefund(currentPurchOrderLines, ticket);
      }
    }
  }

  /**
   * Creates a ticket if late delivery occurs.
   *
   * @param deliveryNotes all deliverynotes from the brokerage process
   */
  private void lateDelivery(Set<Vertex> deliveryNotes) {
    Set<Edge> lateSalesOrderLines = Sets.newHashSet();

    // Iterate over all delivery notes and take the sales order lines of
    // sales orders, which are late
    for (Vertex deliveryNote : deliveryNotes) {
      if (deliveryNote.getPropertyValue(FoodBrokerConstants.DATE_KEY).getLong() >
        salesOrder.getPropertyValue(FoodBrokerConstants.DELIVERYDATE_KEY).getLong()) {
        lateSalesOrderLines.addAll(salesOrderLines);
      }
    }

    // If we have late sales order lines
    if (!lateSalesOrderLines.isEmpty()) {
      // Collect the respective late purch order lines
      Set<Edge> latePurchOrderLines = Sets.newHashSet();
      for (Edge salesOrderLine : lateSalesOrderLines) {
        latePurchOrderLines.add(getCorrespondingPurchOrderLine(salesOrderLine.getId()));
      }
      Calendar calendar = Calendar.getInstance();
      calendar.setTimeInMillis(salesOrder.getPropertyValue(FoodBrokerConstants.DELIVERYDATE_KEY).getLong());
      calendar.add(Calendar.DATE, 1);
      long createdDate = calendar.getTimeInMillis();

      // Create ticket and process refunds
      Vertex ticket = newTicket(FoodBrokerConstants.LATEDELIVERY_TICKET_PROBLEM, createdDate);
      grantSalesRefund(lateSalesOrderLines, ticket);
      claimPurchRefund(latePurchOrderLines, ticket);
    }
  }

  /**
   * Create the ticket itself and corresponding master data with edges.
   *
   * @param problem the reason for the ticket, either bad quality or late delivery
   * @param createdAt creation date
   * @return the ticket
   */
  private Vertex newTicket(String problem, long createdAt) {
    String label = FoodBrokerConstants.TICKET_VERTEX_LABEL;
    Properties properties = new Properties();
    // properties
    properties.set(FoodBrokerConstants.SUPERTYPE_KEY, FoodBrokerConstants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set(FoodBrokerConstants.CREATEDATE_KEY, createdAt);
    properties.set(FoodBrokerConstants.PROBLEM_KEY, problem);
    properties.set(FoodBrokerConstants.ERPSONUM_KEY, salesOrder.getId().toString());

    GradoopId employeeId = getRandomEntryFromArray(employeeList);
    Vertex employee = employeeIndex.get(employeeId);

    GradoopId customerId = getEdgeTargetId(FoodBrokerConstants.RECEIVEDFROM_EDGE_LABEL, salesOrder.getId());

    Vertex ticket = newVertex(label, properties);

    newEdge(FoodBrokerConstants.CONCERNS_EDGE_LABEL, ticket.getId(), salesOrder.getId());
    //new master data, user
    Vertex user = getUserFromEmployee(employee);
    newEdge(FoodBrokerConstants.CREATEDBY_EDGE_LABEL, ticket.getId(), user.getId());
    //new master data, user
    employeeId = getNextEmployee();
    user = getUserFromEmployee(employee);
    newEdge(FoodBrokerConstants.ALLOCATEDTO_EDGE_LABEL, ticket.getId(), user.getId());
    //new master data, client
    Vertex client = getClientFromCustomerId(customerId);
    newEdge(FoodBrokerConstants.OPENEDBY_EDGE_LABEL, ticket.getId(), client.getId());

    return ticket;
  }

  /**
   * Calculates the refund amount to grant and creates a sales invoice with corresponding edges.
   *
   * @param salesOrderLines sales order lines to calculate refund amount
   * @param ticket the ticket created for the refund
   */
  private void grantSalesRefund(Set<Edge> salesOrderLines, Vertex ticket) {
    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality(
      FoodBrokerConstants.ALLOCATEDTO_EDGE_LABEL, ticket.getId(), FoodBrokerConstants.USER_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(FoodBrokerConstants.RECEIVEDFROM_EDGE_LABEL,
      salesOrder.getId(), FoodBrokerConstants.BC_CUSTOMERS));
    //calculate refund
    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(
        influencingMasterQuality, FoodBrokerConstants.TICKET_VERTEX_LABEL,
        FoodBrokerConstants.TI_SALESREFUND_CONFIG_KEY, false);
    BigDecimal refundAmount = BigDecimal.ZERO;
    BigDecimal salesAmount;

    for (Edge salesOrderLine : salesOrderLines) {
      salesAmount = BigDecimal.valueOf(
        salesOrderLine.getPropertyValue(FoodBrokerConstants.QUANTITY_KEY).getInt())
        .multiply(salesOrderLine.getPropertyValue(FoodBrokerConstants.SALESPRICE_KEY).getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      refundAmount = refundAmount.add(salesAmount);
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);
    //create sales invoice if refund is negative
    if (refundAmount.floatValue() < 0) {
      String label = FoodBrokerConstants.SALESINVOICE_VERTEX_LABEL;

      Properties properties = new Properties();
      properties.set(FoodBrokerConstants.SUPERTYPE_KEY, FoodBrokerConstants.SUPERCLASS_VALUE_TRANSACTIONAL);
      properties.set(
        FoodBrokerConstants.DATE_KEY, ticket.getPropertyValue(FoodBrokerConstants.CREATEDATE_KEY).getLong());
      String bid = createBusinessIdentifier(currentId++, FoodBrokerConstants.SALESINVOICE_ACRONYM);
      properties.set(FoodBrokerConstants.SOURCEID_KEY, FoodBrokerConstants.CIT_ACRONYM + "_" + bid);
      properties.set(FoodBrokerConstants.REVENUE_KEY, refundAmount);
      properties.set(FoodBrokerConstants.TEXT_KEY, FoodBrokerConstants.TEXT_CONTENT + ticket.getId());

      Vertex salesInvoice = newVertex(label, properties);

      newEdge(FoodBrokerConstants.CREATEDFOR_EDGE_LABEL, salesInvoice.getId(), salesOrder.getId());
    }
  }

  /**
   * Calculates the refund amount to claim and creates a purch invoice with corresponding edges.
   *
   * @param purchOrderLines purch order lines to calculate refund amount
   * @param ticket the ticket created for the refund
   */
  private void claimPurchRefund(Set<Edge> purchOrderLines, Vertex ticket) {
    GradoopId purchOrderId = purchOrderLines.iterator().next().getSourceId();

    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality(
      FoodBrokerConstants.ALLOCATEDTO_EDGE_LABEL, ticket.getId(), FoodBrokerConstants.USER_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(FoodBrokerConstants.PLACEDAT_EDGE_LABEL,
      purchOrderId, FoodBrokerConstants.BC_VENDORS));
    //calculate refund
    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(
        influencingMasterQuality, FoodBrokerConstants.TICKET_VERTEX_LABEL,
        FoodBrokerConstants.TI_PURCHREFUND_CONFIG_KEY, true);
    BigDecimal refundAmount = BigDecimal.ZERO;
    BigDecimal purchAmount;

    for (Edge purchOrderLine : purchOrderLines) {
      purchAmount = BigDecimal.valueOf(
        purchOrderLine.getPropertyValue(FoodBrokerConstants.QUANTITY_KEY).getInt())
        .multiply(purchOrderLine.getPropertyValue(FoodBrokerConstants.PURCHPRICE_KEY).getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      refundAmount = refundAmount.add(purchAmount);
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);
    //create purch invoice if refund is negative
    if (refundAmount.floatValue() < 0) {
      String label = FoodBrokerConstants.PURCHINVOICE_VERTEX_LABEL;
      Properties properties = new Properties();

      properties.set(FoodBrokerConstants.SUPERTYPE_KEY, FoodBrokerConstants.SUPERCLASS_VALUE_TRANSACTIONAL);
      properties.set(
        FoodBrokerConstants.DATE_KEY, ticket.getPropertyValue(FoodBrokerConstants.CREATEDATE_KEY).getLong());
      String bid = createBusinessIdentifier(
        currentId++, FoodBrokerConstants.PURCHINVOICE_ACRONYM);
      properties.set(FoodBrokerConstants.SOURCEID_KEY, FoodBrokerConstants.CIT_ACRONYM + "_" + bid);
      properties.set(FoodBrokerConstants.EXPENSE_KEY, refundAmount);
      properties.set(FoodBrokerConstants.TEXT_KEY, FoodBrokerConstants.TEXT_CONTENT + ticket.getId());

      Vertex purchInvoice = newVertex(label, properties);

      newEdge(FoodBrokerConstants.CREATEDFOR_EDGE_LABEL, purchInvoice.getId(), purchOrderId);
    }
  }

  /**
   * Returns set of all vertices with the given label.
   *
   * @param transaction the graph transaction containing the vertices
   * @param label the label to be searched on
   * @return set of vertices with the given label
   */
  private Set<Vertex> getVertexByLabel(GraphTransaction transaction, String label) {
    Set<Vertex> vertices = Sets.newHashSet();

    for (Vertex vertex : transaction.getVertices()) {
      if (vertex.getLabel().equals(label)) {
        vertices.add(vertex);
      }
    }
    return vertices;
  }

  /**
   * Returns set of all edges with the given label.
   *
   * @param transaction the graph transaction containing the edges
   * @param label the label to be searched on
   * @return set of edges with the given label
   */
  private Set<Edge> getEdgesByLabel(GraphTransaction transaction, String label) {
    Set<Edge> edges = Sets.newHashSet();
    for (Edge edge : transaction.getEdges()) {
      if (edge.getLabel().equals(label)) {
        edges.add(edge);
      }
    }
    return edges;
  }

  /**
   * Returns set of all purch order lines where the source id is equal to the given purch order id.
   *
   * @param purchOrderId gradoop id of the purch order
   * @return set of purch order lines
   */
  private Set<Edge> getPurchOrderLinesByPurchOrder(GradoopId purchOrderId) {
    Set<Edge> currentPurchOrderLines = Sets.newHashSet();
    for (Edge purchOrderLine : purchOrderLines) {
      if (purchOrderId.equals(purchOrderLine.getSourceId())) {
        currentPurchOrderLines.add(purchOrderLine);
      }
    }
    return currentPurchOrderLines;
  }

  /**
   * Returns all purch order lines which correspond to the given sales order line id.
   *
   * @param salesOrderLineId gradoop id of the sales order line
   * @return set of sales oder lines
   */
  private Edge getCorrespondingPurchOrderLine(GradoopId salesOrderLineId) {
    for (Edge purchOrderLine : purchOrderLines) {
      if (purchOrderLine.getPropertyValue("salesOrderLine").getString()
        .equals(salesOrderLineId.toString())) {
        return purchOrderLine;
      }
    }
    return null;
  }

  /**
   * Returns the sales order line which correspond to the given purch order line id.
   *
   * @param purchOrderLineId gradoop id of the purch order line
   * @return sales order line
   */
  private Edge getCorrespondingSalesOrderLine(GradoopId purchOrderLineId) {
    for (Edge salesOrderLine : salesOrderLines) {
      if (salesOrderLine.getPropertyValue("purchOrderLine").getString()
        .equals(purchOrderLineId.toString())) {
        return salesOrderLine;
      }
    }
    return null;
  }

  /**
   * Creates, if not already existing, and returns the corresponding user vertex to the given
   * employee id.
   *
   * @param employee gradoop id of the employee
   * @return the vertex representing an user
   */
  private Vertex getUserFromEmployee(Vertex employee) {
    if (masterDataMap.containsKey(employee)) {
      return masterDataMap.get(employee);
    } else {
      //create properties
      Properties properties;
      properties = employee.getProperties();
      String sourceIdKey = properties.get(FoodBrokerConstants.SOURCEID_KEY).getString();
      sourceIdKey = sourceIdKey.replace(FoodBrokerConstants.EMPLOYEE_ACRONYM, FoodBrokerConstants.USER_ACRONYM);
      sourceIdKey = sourceIdKey.replace(FoodBrokerConstants.ERP_ACRONYM, FoodBrokerConstants.CIT_ACRONYM);
      properties.set(FoodBrokerConstants.SOURCEID_KEY, sourceIdKey);
      properties.set(FoodBrokerConstants.ERPEMPLNUM_KEY, employee.getId().toString());
      String email = properties.get(FoodBrokerConstants.NAME_KEY).getString();
      email = email.replace(" ", ".").toLowerCase();
      email += "@biiig.org";
      properties.set(FoodBrokerConstants.EMAIL_KEY, email);
      //create the vertex and store it in a map for fast access
      Vertex user = vertexFactory.createVertex(FoodBrokerConstants.USER_VERTEX_LABEL, properties, graphIds);
      masterDataMap.put(employee.getId(), user);
      userMap.put(user.getId(), user.getPropertyValue(FoodBrokerConstants.QUALITY_KEY).getFloat());

//      newEdge(FoodBrokerConstants.SAMEAS_EDGE_LABEL, user.getId(), employee.getId());

      graph.getVertices().add(user);

      return user;
    }
  }

  /**
   * Creates, if not already existing, and returns the corresponding client vertex to the given
   * customer id.
   *
   * @param customerId gradoop id of the customer
   * @return the vertex representing a client
   */
  private Vertex getClientFromCustomerId(GradoopId customerId) {
    Vertex client;

    if (masterDataMap.containsKey(customerId)) {
      client = masterDataMap.get(customerId);
    } else {
      //create properties
      Properties properties;
      Vertex customer = graph.getVertexById(customerId);
      properties = customer.getProperties();
      String sourceIdKey = properties.get(FoodBrokerConstants.SOURCEID_KEY).getString();
      sourceIdKey = sourceIdKey.replace(FoodBrokerConstants.CUSTOMER_ACRONYM, FoodBrokerConstants.CLIENT_ACRONYM);
      sourceIdKey = sourceIdKey.replace(FoodBrokerConstants.ERP_ACRONYM, FoodBrokerConstants.CIT_ACRONYM);
      properties.set(FoodBrokerConstants.SOURCEID_KEY, sourceIdKey);
      properties.set(FoodBrokerConstants.ERPCUSTNUM_KEY, customer.getId().toString());
      properties.set(FoodBrokerConstants.CONTACTPHONE_KEY, "0123456789");
      properties.set(FoodBrokerConstants.ACCOUNT_KEY, "CL" + customer.getId().toString());
      //create the vertex and store it in a map for fast access
      client = vertexFactory.createVertex(
        FoodBrokerConstants.CLIENT_VERTEX_LABEL, properties, graphIds);
      masterDataMap.put(customerId, client);

//      newEdge(FoodBrokerConstants.SAMEAS_EDGE_LABEL, client.getId(), customerId);
    }

    graph.getVertices().add(client);

    return client;
  }

  /**
   * Creates a map from label and source id of an edge to a set of all edges which meet the
   * criteria.
   *
   * @param transaction the graph transaction containing all the edges
   * @return HashMap from (String, GradoopId) -> Set(Edge)
   */
  private Map<Tuple2<String, GradoopId>, Set<Edge>> createEdgeMap(GraphTransaction transaction) {
    Map<Tuple2<String, GradoopId>, Set<Edge>> edgeMap = Maps.newHashMap();
    Set<Edge> edges;
    Tuple2<String, GradoopId> key;
    for (Edge edge : transaction.getEdges()) {
      edges = Sets.newHashSet();
      key = new Tuple2<>(edge.getLabel(), edge.getSourceId());
      if (edgeMap.containsKey(key)) {
        edges.addAll(edgeMap.get(key));
      }
      edges.add(edge);
      edgeMap.put(key, edges);
    }
    return edgeMap;
  }
}
