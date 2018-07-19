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
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerAcronyms;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfigurationKeys;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerBroadcastNames;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerEdgeLabels;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerPropertyKeys;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerPropertyValues;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerVertexLabels;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

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

    //init new maps
    vertexMap = Maps.newHashMap();
    masterDataMap = Maps.newHashMap();
    userMap = Maps.newHashMap();
    graphIds = GradoopIdSet.fromExisting(graph.getGraphHead().getId());

    boolean confirmed = false;

    for (Vertex vertex : graph.getVertices()) {
      if (vertex.getLabel().equals(FoodBrokerVertexLabels.SALESORDER_VERTEX_LABEL)) {
        confirmed = true;
        break;
      }
    }

    if (confirmed) {
      edgeMap = createEdgeMap(graph);
      //get needed transactional objects created during brokerage process
      Set<Vertex> deliveryNotes =
        getVertexByLabel(graph, FoodBrokerVertexLabels.DELIVERYNOTE_VERTEX_LABEL);
      salesOrderLines = getEdgesByLabel(graph, FoodBrokerEdgeLabels.SALESORDERLINE_EDGE_LABEL);
      purchOrderLines = getEdgesByLabel(graph, FoodBrokerEdgeLabels.PURCHORDERLINE_EDGE_LABEL);
      salesOrder =
        getVertexByLabel(graph, FoodBrokerVertexLabels.SALESORDER_VERTEX_LABEL).iterator().next();

      //the complaint handling process
      badQuality(deliveryNotes, graph);
      lateDelivery(deliveryNotes, graph);
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
   * @param graph current case
   */
  private void badQuality(Set<Vertex> deliveryNotes, GraphTransaction graph) {
    GradoopId purchOrderId;
    List<Float> influencingMasterQuality;
    Set<Edge> currentPurchOrderLines;
    Set<Edge> badSalesOrderLines;

    for (Vertex deliveryNote : deliveryNotes) {
      influencingMasterQuality = Lists.newArrayList();
      badSalesOrderLines = Sets.newHashSet();
      //get the corresponding purch order and purch order lines
      purchOrderId =
        getEdgeTargetId(FoodBrokerEdgeLabels.CONTAINS_EDGE_LABEL, deliveryNote.getId());
      currentPurchOrderLines = getPurchOrderLinesByPurchOrder(purchOrderId);

      for (Edge purchOrderLine : currentPurchOrderLines) {
        influencingMasterQuality.add(getQuality(productIndex, purchOrderLine.getTargetId()));
      }
      int containedProducts = influencingMasterQuality.size();
      // increase relative influence of vendor and logistics
      for (int i = 1; i <= containedProducts / 2; i++) {
        influencingMasterQuality.add(
          getEdgeTargetQuality(
            FoodBrokerEdgeLabels.OPERATEDBY_EDGE_LABEL,
            deliveryNote.getId(),
            FoodBrokerBroadcastNames.BC_LOGISTICS)
        );
        influencingMasterQuality.add(
          getEdgeTargetQuality(
            FoodBrokerEdgeLabels.PLACEDAT_EDGE_LABEL,
            purchOrderId,
            FoodBrokerBroadcastNames.BC_VENDORS)
        );
      }
      if (config
        .happensTransitionConfiguration(
          influencingMasterQuality,
          FoodBrokerVertexLabels.TICKET_VERTEX_LABEL,
          FoodBrokerConfigurationKeys.TI_BADQUALITYPROBABILITY_CONFIG_KEY,
          false
        )) {

        for (Edge purchOrderLine : currentPurchOrderLines) {
          badSalesOrderLines.add(getCorrespondingSalesOrderLine(purchOrderLine.getId()));
        }

        Vertex ticket = newTicket(
          FoodBrokerPropertyValues.BADQUALITY_TICKET_PROBLEM,
          deliveryNote.getPropertyValue(FoodBrokerPropertyKeys.DATE_KEY).getLong(), graph);

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
   * @param graph current case
   */
  private void lateDelivery(Set<Vertex> deliveryNotes, GraphTransaction graph) {
    Set<Edge> lateSalesOrderLines = Sets.newHashSet();

    // Iterate over all delivery notes and take the sales order lines of
    // sales orders, which are late
    for (Vertex deliveryNote : deliveryNotes) {
      if (deliveryNote.getPropertyValue(FoodBrokerPropertyKeys.DATE_KEY).getLong() >
        salesOrder.getPropertyValue(FoodBrokerPropertyKeys.DELIVERYDATE_KEY).getLong()) {
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
      calendar.setTimeInMillis(
        salesOrder.getPropertyValue(FoodBrokerPropertyKeys.DELIVERYDATE_KEY).getLong());
      calendar.add(Calendar.DATE, 1);
      long createdDate = calendar.getTimeInMillis();

      // Create ticket and process refunds
      Vertex ticket =
        newTicket(FoodBrokerPropertyValues.LATEDELIVERY_TICKET_PROBLEM, createdDate, graph);
      grantSalesRefund(lateSalesOrderLines, ticket);
      claimPurchRefund(latePurchOrderLines, ticket);
    }
  }

  /**
   * Create the ticket itself and corresponding master data with edges.
   *
   * @param problem the reason for the ticket, either bad quality or late delivery
   * @param createdAt creation date
   * @param graph current case
   * @return the ticket
   */
  private Vertex newTicket(String problem, long createdAt, GraphTransaction graph) {
    String label = FoodBrokerVertexLabels.TICKET_VERTEX_LABEL;
    Properties properties = new Properties();
    // properties
    properties.set(
      FoodBrokerPropertyKeys.SUPERTYPE_KEY,
      FoodBrokerPropertyValues.SUPERCLASS_VALUE_TRANSACTIONAL
    );
    properties.set(FoodBrokerPropertyKeys.CREATEDATE_KEY, createdAt);
    properties.set(FoodBrokerPropertyKeys.PROBLEM_KEY, problem);
    properties.set(FoodBrokerPropertyKeys.ERPSONUM_KEY, salesOrder.getId().toString());

    GradoopId employeeId = getRandomEntryFromArray(employeeList);
    Vertex employee = employeeIndex.get(employeeId);

    GradoopId customerId =
      getEdgeTargetId(FoodBrokerEdgeLabels.RECEIVEDFROM_EDGE_LABEL, salesOrder.getId());

    Vertex ticket = newVertex(label, properties);

    newEdge(FoodBrokerEdgeLabels.CONCERNS_EDGE_LABEL, ticket.getId(), salesOrder.getId());
    //new master data, user
    Vertex user = getUserFromEmployee(employee, graph);
    newEdge(FoodBrokerEdgeLabels.CREATEDBY_EDGE_LABEL, ticket.getId(), user.getId());
    //new master data, user
    user = getUserFromEmployee(employee, graph);
    newEdge(FoodBrokerEdgeLabels.ALLOCATEDTO_EDGE_LABEL, ticket.getId(), user.getId());
    //new master data, client
    Vertex client = getClientFromCustomerId(customerId, graph);
    newEdge(FoodBrokerEdgeLabels.OPENEDBY_EDGE_LABEL, ticket.getId(), client.getId());

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
      FoodBrokerEdgeLabels.ALLOCATEDTO_EDGE_LABEL,
      ticket.getId(),
      FoodBrokerBroadcastNames.USER_MAP)
    );
    influencingMasterQuality.add(getEdgeTargetQuality(FoodBrokerEdgeLabels.RECEIVEDFROM_EDGE_LABEL,
      salesOrder.getId(), FoodBrokerBroadcastNames.BC_CUSTOMERS));
    //calculate refund
    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(
        influencingMasterQuality, FoodBrokerVertexLabels.TICKET_VERTEX_LABEL,
        FoodBrokerConfigurationKeys.TI_SALESREFUND_CONFIG_KEY, false);
    BigDecimal refundAmount = BigDecimal.ZERO;
    BigDecimal salesAmount;

    for (Edge salesOrderLine : salesOrderLines) {
      salesAmount = BigDecimal.valueOf(
        salesOrderLine.getPropertyValue(FoodBrokerPropertyKeys.QUANTITY_KEY).getInt())
        .multiply(
          salesOrderLine.getPropertyValue(FoodBrokerPropertyKeys.SALESPRICE_KEY).getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      refundAmount = refundAmount.add(salesAmount);
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);
    //create sales invoice if refund is negative
    if (refundAmount.floatValue() < 0) {
      String label = FoodBrokerVertexLabels.SALESINVOICE_VERTEX_LABEL;

      Properties properties = new Properties();
      properties.set(
        FoodBrokerPropertyKeys.SUPERTYPE_KEY,
        FoodBrokerPropertyValues.SUPERCLASS_VALUE_TRANSACTIONAL);
      properties.set(
        FoodBrokerPropertyKeys.DATE_KEY,
        ticket.getPropertyValue(FoodBrokerPropertyKeys.CREATEDATE_KEY).getLong());
      String bid = createBusinessIdentifier(currentId++, FoodBrokerAcronyms.SALESINVOICE_ACRONYM);
      properties.set(
        FoodBrokerPropertyKeys.SOURCEID_KEY, FoodBrokerAcronyms.CIT_ACRONYM + "_" + bid);
      properties.set(FoodBrokerPropertyKeys.REVENUE_KEY, refundAmount);
      properties.set(
        FoodBrokerPropertyKeys.TEXT_KEY, FoodBrokerPropertyValues.TEXT_CONTENT + ticket.getId());

      Vertex salesInvoice = newVertex(label, properties);

      newEdge(FoodBrokerEdgeLabels.CREATEDFOR_EDGE_LABEL, salesInvoice.getId(), salesOrder.getId());
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
      FoodBrokerEdgeLabels.ALLOCATEDTO_EDGE_LABEL,
      ticket.getId(),
      FoodBrokerBroadcastNames.USER_MAP)
    );
    influencingMasterQuality.add(getEdgeTargetQuality(FoodBrokerEdgeLabels.PLACEDAT_EDGE_LABEL,
      purchOrderId, FoodBrokerBroadcastNames.BC_VENDORS));
    //calculate refund
    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(
        influencingMasterQuality, FoodBrokerVertexLabels.TICKET_VERTEX_LABEL,
        FoodBrokerConfigurationKeys.TI_PURCHREFUND_CONFIG_KEY, true);
    BigDecimal refundAmount = BigDecimal.ZERO;
    BigDecimal purchAmount;

    for (Edge purchOrderLine : purchOrderLines) {
      purchAmount = BigDecimal.valueOf(
        purchOrderLine.getPropertyValue(FoodBrokerPropertyKeys.QUANTITY_KEY).getInt())
        .multiply(purchOrderLine.getPropertyValue(
          FoodBrokerPropertyKeys.PURCHPRICE_KEY).getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      refundAmount = refundAmount.add(purchAmount);
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);
    //create purch invoice if refund is negative
    if (refundAmount.floatValue() < 0) {
      String label = FoodBrokerVertexLabels.PURCHINVOICE_VERTEX_LABEL;
      Properties properties = new Properties();

      properties.set(FoodBrokerPropertyKeys.SUPERTYPE_KEY,
        FoodBrokerPropertyValues.SUPERCLASS_VALUE_TRANSACTIONAL);
      properties.set(
        FoodBrokerPropertyKeys.DATE_KEY,
        ticket.getPropertyValue(FoodBrokerPropertyKeys.CREATEDATE_KEY).getLong());
      String bid = createBusinessIdentifier(
        currentId++, FoodBrokerAcronyms.PURCHINVOICE_ACRONYM);
      properties.set(
        FoodBrokerPropertyKeys.SOURCEID_KEY, FoodBrokerAcronyms.CIT_ACRONYM + "_" + bid);
      properties.set(FoodBrokerPropertyKeys.EXPENSE_KEY, refundAmount);
      properties.set(FoodBrokerPropertyKeys.TEXT_KEY,
        FoodBrokerPropertyValues.TEXT_CONTENT + ticket.getId());

      Vertex purchInvoice = newVertex(label, properties);

      newEdge(FoodBrokerEdgeLabels.CREATEDFOR_EDGE_LABEL, purchInvoice.getId(), purchOrderId);
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
   * @param graph current case
   * @return the vertex representing an user
   */
  private Vertex getUserFromEmployee(Vertex employee, GraphTransaction graph) {
    if (masterDataMap.containsKey(employee.getId())) {
      return masterDataMap.get(employee.getId());
    } else {
      //create properties
      Properties properties;
      properties = employee.getProperties();
      String sourceIdKey = properties.get(FoodBrokerPropertyKeys.SOURCEID_KEY).getString();
      sourceIdKey = sourceIdKey
        .replace(FoodBrokerAcronyms.EMPLOYEE_ACRONYM, FoodBrokerAcronyms.USER_ACRONYM);
      sourceIdKey = sourceIdKey
        .replace(FoodBrokerAcronyms.ERP_ACRONYM, FoodBrokerAcronyms.CIT_ACRONYM);
      properties.set(FoodBrokerPropertyKeys.SOURCEID_KEY, sourceIdKey);
      properties.set(FoodBrokerPropertyKeys.ERPEMPLNUM_KEY, employee.getId().toString());
      String email = properties.get(FoodBrokerPropertyKeys.NAME_KEY).getString();
      email = email.replace(" ", ".").toLowerCase();
      email += "@biiig.org";
      properties.set(FoodBrokerPropertyKeys.EMAIL_KEY, email);
      //create the vertex and store it in a map for fast access
      Vertex user = vertexFactory
        .createVertex(FoodBrokerVertexLabels.USER_VERTEX_LABEL, properties, graphIds);
      masterDataMap.put(employee.getId(), user);
      userMap
        .put(user.getId(), user.getPropertyValue(FoodBrokerPropertyKeys.QUALITY_KEY).getFloat());

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
   * @param graph current case
   * @return the vertex representing a client
   */
  private Vertex getClientFromCustomerId(GradoopId customerId, GraphTransaction graph) {
    Vertex client;

    if (masterDataMap.containsKey(customerId)) {
      client = masterDataMap.get(customerId);
    } else {
      //create properties
      Properties properties;
      Vertex customer = graph.getVertexById(customerId);
      properties = customer.getProperties();
      String sourceIdKey = properties.get(FoodBrokerPropertyKeys.SOURCEID_KEY).getString();
      sourceIdKey = sourceIdKey
        .replace(FoodBrokerAcronyms.CUSTOMER_ACRONYM, FoodBrokerAcronyms.CLIENT_ACRONYM);
      sourceIdKey = sourceIdKey
        .replace(FoodBrokerAcronyms.ERP_ACRONYM, FoodBrokerAcronyms.CIT_ACRONYM);
      properties.set(FoodBrokerPropertyKeys.SOURCEID_KEY, sourceIdKey);
      properties.set(FoodBrokerPropertyKeys.ERPCUSTNUM_KEY, customer.getId().toString());
      properties.set(FoodBrokerPropertyKeys.CONTACTPHONE_KEY, "0123456789");
      properties.set(FoodBrokerPropertyKeys.ACCOUNT_KEY, "CL" + customer.getId().toString());
      //create the vertex and store it in a map for fast access
      client = vertexFactory.createVertex(
        FoodBrokerVertexLabels.CLIENT_VERTEX_LABEL, properties, graphIds);
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
