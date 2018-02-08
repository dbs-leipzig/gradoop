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
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
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
  implements MapFunction<GraphTransaction, Tuple2<GraphTransaction, Set<Vertex>>> {
  /**
   * List of employees. Used to create new user.
   */
  private List<Vertex> employees;
  /**
   * List of customers. Used to create new clients.
   */
  private List<Vertex> customers;
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
    employees = getRuntimeContext().getBroadcastVariable(Constants.EMPLOYEE_VERTEX_LABEL);
    customers = getRuntimeContext().getBroadcastVariable(Constants.CUSTOMER_VERTEX_LABEL);
  }

  @Override
  public Tuple2<GraphTransaction, Set<Vertex>> map(GraphTransaction graph) throws Exception {

    //init new maps
    vertexMap = Maps.newHashMap();
    masterDataMap = Maps.newHashMap();
    userMap = Maps.newHashMap();
    graphIds = GradoopIdSet.fromExisting(graph.getGraphHead().getId());

    boolean confirmed = false;

    for (Vertex vertex : graph.getVertices()) {
      if (vertex.getLabel().equals(Constants.SALESORDER_VERTEX_LABEL)) {
        confirmed = true;
        break;
      }
    }

    if (confirmed) {
      edgeMap = createEdgeMap(graph);
      //get needed transactional objects created during brokerage process
      Set<Vertex> deliveryNotes = getVertexByLabel(graph, Constants.DELIVERYNOTE_VERTEX_LABEL);
      salesOrderLines = getEdgesByLabel(graph, Constants.SALESORDERLINE_EDGE_LABEL);
      purchOrderLines = getEdgesByLabel(graph, Constants.PURCHORDERLINE_EDGE_LABEL);
      salesOrder = getVertexByLabel(graph, Constants.SALESORDER_VERTEX_LABEL).iterator().next();

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

    return new Tuple2<>(graph, getMasterData());
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
      purchOrderId = getEdgeTargetId(Constants.CONTAINS_EDGE_LABEL, deliveryNote.getId());
      currentPurchOrderLines = getPurchOrderLinesByPurchOrder(purchOrderId);

      for (Edge purchOrderLine : currentPurchOrderLines) {
        influencingMasterQuality.add(productQualityMap.get(purchOrderLine.getTargetId()));
      }
      int containedProducts = influencingMasterQuality.size();
      // increase relative influence of vendor and logistics
      for (int i = 1; i <= containedProducts / 2; i++) {
        influencingMasterQuality.add(getEdgeTargetQuality(
          Constants.OPERATEDBY_EDGE_LABEL, deliveryNote.getId(), Constants.LOGISTIC_MAP_BC));
        influencingMasterQuality.add(getEdgeTargetQuality(Constants.PLACEDAT_EDGE_LABEL,
          purchOrderId, Constants.VENDOR_MAP_BC));
      }
      if (config.happensTransitionConfiguration(
        influencingMasterQuality, Constants.TICKET_VERTEX_LABEL,
        Constants.TI_BADQUALITYPROBABILITY_CONFIG_KEY, false)) {

        for (Edge purchOrderLine : currentPurchOrderLines) {
          badSalesOrderLines.add(getCorrespondingSalesOrderLine(purchOrderLine.getId()));
        }

        Vertex ticket = newTicket(
          Constants.BADQUALITY_TICKET_PROBLEM,
          deliveryNote.getPropertyValue(Constants.DATE_KEY).getLong());
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
      if (deliveryNote.getPropertyValue(Constants.DATE_KEY).getLong() >
        salesOrder.getPropertyValue(Constants.DELIVERYDATE_KEY).getLong()) {
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
      calendar.setTimeInMillis(salesOrder.getPropertyValue(Constants.DELIVERYDATE_KEY).getLong());
      calendar.add(Calendar.DATE, 1);
      long createdDate = calendar.getTimeInMillis();

      // Create ticket and process refunds
      Vertex ticket = newTicket(Constants.LATEDELIVERY_TICKET_PROBLEM, createdDate);
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
    String label = Constants.TICKET_VERTEX_LABEL;
    Properties properties = new Properties();
    // properties
    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set(Constants.CREATEDATE_KEY, createdAt);
    properties.set(Constants.PROBLEM_KEY, problem);
    properties.set(Constants.ERPSONUM_KEY, salesOrder.getId().toString());

    GradoopId employeeId = getNextEmployee();
    GradoopId customerId = getEdgeTargetId(Constants.RECEIVEDFROM_EDGE_LABEL, salesOrder.getId());

    Vertex ticket = newVertex(label, properties);

    newEdge(Constants.CONCERNS_EDGE_LABEL, ticket.getId(), salesOrder.getId());
    //new master data, user
    Vertex user = getUserFromEmployeeId(employeeId);
    newEdge(Constants.CREATEDBY_EDGE_LABEL, ticket.getId(), user.getId());
    //new master data, user
    employeeId = getNextEmployee();
    user = getUserFromEmployeeId(employeeId);
    newEdge(Constants.ALLOCATEDTO_EDGE_LABEL, ticket.getId(), user.getId());
    //new master data, client
    Vertex client = getClientFromCustomerId(customerId);
    newEdge(Constants.OPENEDBY_EDGE_LABEL, ticket.getId(), client.getId());

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
      Constants.ALLOCATEDTO_EDGE_LABEL, ticket.getId(), Constants.USER_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(Constants.RECEIVEDFROM_EDGE_LABEL,
      salesOrder.getId(), Constants.CUSTOMER_MAP_BC));
    //calculate refund
    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(
        influencingMasterQuality, Constants.TICKET_VERTEX_LABEL,
        Constants.TI_SALESREFUND_CONFIG_KEY, false);
    BigDecimal refundAmount = BigDecimal.ZERO;
    BigDecimal salesAmount;

    for (Edge salesOrderLine : salesOrderLines) {
      salesAmount = BigDecimal.valueOf(
        salesOrderLine.getPropertyValue(Constants.QUANTITY_KEY).getInt())
        .multiply(salesOrderLine.getPropertyValue(Constants.SALESPRICE_KEY).getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      refundAmount = refundAmount.add(salesAmount);
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);
    //create sales invoice if refund is negative
    if (refundAmount.floatValue() < 0) {
      String label = Constants.SALESINVOICE_VERTEX_LABEL;

      Properties properties = new Properties();
      properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
      properties.set(
        Constants.DATE_KEY, ticket.getPropertyValue(Constants.CREATEDATE_KEY).getLong());
      String bid = createBusinessIdentifier(currentId++, Constants.SALESINVOICE_ACRONYM);
      properties.set(Constants.SOURCEID_KEY, Constants.CIT_ACRONYM + "_" + bid);
      properties.set(Constants.REVENUE_KEY, refundAmount);
      properties.set(Constants.TEXT_KEY, Constants.TEXT_CONTENT + ticket.getId());

      Vertex salesInvoice = newVertex(label, properties);

      newEdge(Constants.CREATEDFOR_EDGE_LABEL, salesInvoice.getId(), salesOrder.getId());
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
      Constants.ALLOCATEDTO_EDGE_LABEL, ticket.getId(), Constants.USER_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(Constants.PLACEDAT_EDGE_LABEL,
      purchOrderId, Constants.VENDOR_MAP_BC));
    //calculate refund
    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(
        influencingMasterQuality, Constants.TICKET_VERTEX_LABEL,
        Constants.TI_PURCHREFUND_CONFIG_KEY, true);
    BigDecimal refundAmount = BigDecimal.ZERO;
    BigDecimal purchAmount;

    for (Edge purchOrderLine : purchOrderLines) {
      purchAmount = BigDecimal.valueOf(
        purchOrderLine.getPropertyValue(Constants.QUANTITY_KEY).getInt())
        .multiply(purchOrderLine.getPropertyValue(Constants.PURCHPRICE_KEY).getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      refundAmount = refundAmount.add(purchAmount);
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);
    //create purch invoice if refund is negative
    if (refundAmount.floatValue() < 0) {
      String label = Constants.PURCHINVOICE_VERTEX_LABEL;
      Properties properties = new Properties();

      properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
      properties.set(
        Constants.DATE_KEY, ticket.getPropertyValue(Constants.CREATEDATE_KEY).getLong());
      String bid = createBusinessIdentifier(
        currentId++, Constants.PURCHINVOICE_ACRONYM);
      properties.set(Constants.SOURCEID_KEY, Constants.CIT_ACRONYM + "_" + bid);
      properties.set(Constants.EXPENSE_KEY, refundAmount);
      properties.set(Constants.TEXT_KEY, Constants.TEXT_CONTENT + ticket.getId());

      Vertex purchInvoice = newVertex(label, properties);

      newEdge(Constants.CREATEDFOR_EDGE_LABEL, purchInvoice.getId(), purchOrderId);
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
   * Returns the vertex to the given customer id.
   *
   * @param id gradoop id of the customer
   * @return the vertex representing a customer
   */
  private Vertex getCustomerById(GradoopId id) {
    for (Vertex vertex : customers) {
      if (vertex.getId().equals(id)) {
        return vertex;
      }
    }
    return null;
  }

  /**
   * Returns the vertex to the given employee id.
   *
   * @param id gradoop id of the employee
   * @return the vertex representing an employee
   */
  private Vertex getEmployeeById(GradoopId id) {
    for (Vertex vertex : employees) {
      if (vertex.getId().equals(id)) {
        return vertex;
      }
    }
    return null;
  }

  /**
   * Creates, if not already existing, and returns the corresponding user vertex to the given
   * employee id.
   *
   * @param employeeId gradoop id of the employee
   * @return the vertex representing an user
   */
  private Vertex getUserFromEmployeeId(GradoopId employeeId) {
    if (masterDataMap.containsKey(employeeId)) {
      return masterDataMap.get(employeeId);
    } else {
      //create properties
      Vertex employee = getEmployeeById(employeeId);
      if (employee != null && employee.getProperties() != null) {
        Properties properties = employee.getProperties();
        String sourceIdKey = properties.get(Constants.SOURCEID_KEY).getString();
        sourceIdKey = sourceIdKey.replace(Constants.EMPLOYEE_ACRONYM, Constants.USER_ACRONYM);
        sourceIdKey = sourceIdKey.replace(Constants.ERP_ACRONYM, Constants.CIT_ACRONYM);
        properties.set(Constants.SOURCEID_KEY, sourceIdKey);
        properties.set(Constants.ERPEMPLNUM_KEY, employee.getId().toString());
        String email = properties.get(Constants.NAME_KEY).getString();
        email = email.replace(" ", ".").toLowerCase();
        email += "@biiig.org";
        properties.set(Constants.EMAIL_KEY, email);
        //create the vertex and store it in a map for fast access
        Vertex user = vertexFactory.createVertex(Constants.USER_VERTEX_LABEL, properties, graphIds);
        masterDataMap.put(employeeId, user);
        userMap.put(user.getId(), user.getPropertyValue(Constants.QUALITY_KEY).getFloat());

        newEdge(Constants.SAMEAS_EDGE_LABEL, user.getId(), employeeId);
        return user;
      }
      throw new IllegalArgumentException("No employee for id: " + employeeId);
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
    if (masterDataMap.containsKey(customerId)) {
      return masterDataMap.get(customerId);
    } else {
      //create properties
      Vertex customer = getCustomerById(customerId);
      if (customer != null && customer.getProperties() != null) {
        Properties properties = customer.getProperties();
        String sourceIdKey = properties.get(Constants.SOURCEID_KEY).getString();
        sourceIdKey = sourceIdKey.replace(Constants.CUSTOMER_ACRONYM, Constants.CLIENT_ACRONYM);
        sourceIdKey = sourceIdKey.replace(Constants.ERP_ACRONYM, Constants.CIT_ACRONYM);
        properties.set(Constants.SOURCEID_KEY, sourceIdKey);
        properties.set(Constants.ERPCUSTNUM_KEY, customer.getId().toString());
        properties.set(Constants.CONTACTPHONE_KEY, "0123456789");
        properties.set(Constants.ACCOUNT_KEY, "CL" + customer.getId().toString());
        //create the vertex and store it in a map for fast access
        Vertex client = vertexFactory.createVertex(
          Constants.CLIENT_VERTEX_LABEL, properties, graphIds);
        masterDataMap.put(customerId, client);

        newEdge(Constants.SAMEAS_EDGE_LABEL, client.getId(), customerId);
        return client;
      }
      throw new IllegalArgumentException("No customer for id: " + customerId);
    }
  }

  /**
   * Returns set of vertices which contains all in this process created master data.
   *
   * @return set of vertices
   */
  private Set<Vertex> getMasterData() {
    return Sets.newHashSet(masterDataMap.values());
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
