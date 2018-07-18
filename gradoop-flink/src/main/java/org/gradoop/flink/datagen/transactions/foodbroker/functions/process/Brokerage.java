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
import org.apache.flink.api.common.functions.MapFunction;
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
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfigurationKeys;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerBroadcastNames;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerEdgeLabels;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerPropertyKeys;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerPropertyValues;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerVertexLabels;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;


import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Map partition function which spreads the whole brokerage process equally to each worker.
 */
public class Brokerage
  extends AbstractProcess
  implements MapFunction<Long, GraphTransaction> {

  /**
   * Valued constructor
   *
   * @param graphHeadFactory EPGM graph head facroty
   * @param vertexFactory EPGM vertex factory
   * @param edgeFactory EPGM edge factory
   * @param config Foodbroker configuration
   */
  public Brokerage(EPGMGraphHeadFactory<GraphHead> graphHeadFactory,
    EPGMVertexFactory<Vertex> vertexFactory, EPGMEdgeFactory<Edge> edgeFactory,
    FoodBrokerConfig config) {
    super(graphHeadFactory, vertexFactory, edgeFactory, config);
  }

  @Override
  public GraphTransaction map(Long seed)
    throws Exception {
    GraphHead graphHead;
    GraphTransaction graphTransaction;

    LocalDate startDate = config.getStartDate();

    // each seed stands for one created sales quotation

    globalSeed = seed;
    vertexMap = Maps.newHashMap();
    edgeMap = Maps.newHashMap();
    graphHead = graphHeadFactory.createGraphHead();
    graphIds = new GradoopIdSet();
    graphIds.add(graphHead.getId());
    graphTransaction = new GraphTransaction();

    // SalesQuotation
    Vertex salesQuotation = newSalesQuotation(startDate);

    // SalesQuotationLines
    List<Edge> salesQuotationLines = newSalesQuotationLines(salesQuotation);

    if (confirmed(salesQuotation)) {
      // SalesOrder
      Vertex salesOrder = newSalesOrder(salesQuotation);

      // SalesOrderLines
      List<Edge> salesOrderLines = newSalesOrderLines(salesOrder,
        salesQuotationLines);

      // newPurchOrders
      List<Vertex> purchOrders = newPurchOrders(salesOrder, salesOrderLines);

      // PurchOrderLines
      List<Edge> purchOrderLines = newPurchOrderLines(purchOrders, salesOrderLines);

      // DeliveryNotes
      newDeliveryNotes(purchOrders);

      // PurchInvoices
      newPurchInvoices(purchOrderLines);

      // SalesInvoices
      newSalesInvoice(salesOrderLines);
    }
    // fill the graph transaction
    graphTransaction.setGraphHead(graphHead);
    graphTransaction.setVertices(getVertices());
    graphTransaction.setEdges(getEdges());
    return graphTransaction;
  }

  /**
   * Checks if a sales quotation is confirmed.
   *
   * @param salesQuotation the quotation to be checked
   * @return true, if quotation is confirmed
   */
  private boolean confirmed(Vertex salesQuotation) {
    List<Float> influencingMasterQuality = Lists.newArrayList();
    GradoopId employee =
      getEdgeTargetId(FoodBrokerEdgeLabels.SENTBY_EDGE_LABEL, salesQuotation.getId());
    GradoopId customer =
      getEdgeTargetId(FoodBrokerEdgeLabels.SENTTO_EDGE_LABEL, salesQuotation.getId());
    // the additional influence is increased of the two master data objects share the same city
    // or holding
    Float additionalInfluence = getAdditionalInfluence(
      employee, FoodBrokerBroadcastNames.BC_EMPLOYEES,
      customer, FoodBrokerBroadcastNames.BC_CUSTOMERS);

    influencingMasterQuality.add(
      getEdgeTargetQuality(employee, FoodBrokerBroadcastNames.BC_EMPLOYEES) * additionalInfluence);
    influencingMasterQuality.add(
      getEdgeTargetQuality(customer, FoodBrokerBroadcastNames.BC_CUSTOMERS) * additionalInfluence);

    return config.happensTransitionConfiguration(
      influencingMasterQuality, FoodBrokerVertexLabels.SALESQUOTATION_VERTEX_LABEL,
      FoodBrokerConfigurationKeys.SQ_CONFIRMATIONPROBABILITY_CONFIG_KEY, false);
  }

  /**
   * Creates a new sales quotation.
   *
   * @param startDate of the quotation
   * @return vertex representation of a sales quotation
   */
  private Vertex newSalesQuotation(LocalDate startDate) {
    String label = FoodBrokerVertexLabels.SALESQUOTATION_VERTEX_LABEL;
    Properties properties = new Properties();

    String bid = createBusinessIdentifier(currentId++, FoodBrokerAcronyms.SALESQUOTATION_ACRONYM);

    // set properties
    properties.set(
      FoodBrokerPropertyKeys.SUPERTYPE_KEY,
      FoodBrokerPropertyValues.SUPERCLASS_VALUE_TRANSACTIONAL
    );
    properties.set(FoodBrokerPropertyKeys.DATE_KEY, startDate);
    properties.set(FoodBrokerPropertyKeys.SOURCEID_KEY, bid);

    Vertex salesQuotation = newVertex(label, properties);

    // select random employee and customer
    GradoopId rndEmployee = getNextEmployee();
    GradoopId rndCustomer = getNextCustomer();

    newEdge(FoodBrokerEdgeLabels.SENTBY_EDGE_LABEL, salesQuotation.getId(), rndEmployee);
    newEdge(FoodBrokerEdgeLabels.SENTTO_EDGE_LABEL, salesQuotation.getId(), rndCustomer);

    return salesQuotation;
  }

  /**
   * Creates new sales quotation lines.
   *
   * @param salesQuotation quotation, corresponding to the lines
   * @return list of vertices which represent a sales quotation line
   */
  private List<Edge> newSalesQuotationLines(Vertex salesQuotation) {
    List<Edge> salesQuotationLines = Lists.newArrayList();
    Edge salesQuotationLine;
    GradoopId product;

    List<Float> influencingMasterQuality = Lists.newArrayList();
    GradoopId employee =
      getEdgeTargetId(FoodBrokerEdgeLabels.SENTBY_EDGE_LABEL, salesQuotation.getId());
    GradoopId customer =
      getEdgeTargetId(FoodBrokerEdgeLabels.SENTTO_EDGE_LABEL, salesQuotation.getId());
    // the additional influence is increased of the two master data objects share the same city
    // or holding
    Float additionalInfluence = getAdditionalInfluence(
      employee, FoodBrokerBroadcastNames.BC_EMPLOYEES,
      customer, FoodBrokerBroadcastNames.BC_CUSTOMERS);

    influencingMasterQuality.add(
      getEdgeTargetQuality(employee, FoodBrokerBroadcastNames.BC_EMPLOYEES) * additionalInfluence);
    influencingMasterQuality.add(
      getEdgeTargetQuality(customer, FoodBrokerBroadcastNames.BC_CUSTOMERS) * additionalInfluence);

    int numberOfQuotationLines = config.getIntRangeConfigurationValue(
      influencingMasterQuality, FoodBrokerVertexLabels.SALESQUOTATION_VERTEX_LABEL,
      FoodBrokerConfigurationKeys.SQ_LINES_CONFIG_KEY, true);

    // create sales quotation lines based on calculated amount
    for (int i = 0; i < numberOfQuotationLines; i++) {
      product = getNextProduct();
      salesQuotationLine = newSalesQuotationLine(salesQuotation, product);
      salesQuotationLines.add(salesQuotationLine);
    }
    return salesQuotationLines;
  }

  /**
   * Creates a new sales quotation line.
   *
   * @param salesQuotation quotation, corresponding to the lines
   * @param product product, corresponding to the line
   * @return vertex representation of a sales quotation line
   */
  private Edge newSalesQuotationLine(Vertex salesQuotation, GradoopId product) {
    String label = FoodBrokerEdgeLabels.SALESQUOTATIONLINE_EDGE_LABEL;
    Properties properties = new Properties();

    List<Float> influencingMasterQuality = Lists.newArrayList();
    GradoopId employee =
      getEdgeTargetId(FoodBrokerEdgeLabels.SENTBY_EDGE_LABEL, salesQuotation.getId());
    GradoopId customer =
      getEdgeTargetId(FoodBrokerEdgeLabels.SENTTO_EDGE_LABEL, salesQuotation.getId());
    // the additional influence is increased of the two master data objects share the same city
    // or holding
    Float additionalInfluence = getAdditionalInfluence(
      employee, FoodBrokerBroadcastNames.BC_EMPLOYEES,
      customer, FoodBrokerBroadcastNames.BC_CUSTOMERS);

    influencingMasterQuality.add(
      getEdgeTargetQuality(employee, FoodBrokerBroadcastNames.BC_EMPLOYEES) * additionalInfluence);
    influencingMasterQuality.add(
      getEdgeTargetQuality(customer, FoodBrokerBroadcastNames.BC_CUSTOMERS) * additionalInfluence);
    influencingMasterQuality.add(getQuality(productIndex, product));

    // calculate and set the lines properties
    BigDecimal salesMargin = config.getDecimalVariationConfigurationValue(
      influencingMasterQuality, FoodBrokerVertexLabels.SALESQUOTATION_VERTEX_LABEL,
      FoodBrokerConfigurationKeys.SQ_SALESMARGIN_CONFIG_KEY, true);

    influencingMasterQuality.clear();
    int quantity = config.getIntRangeConfigurationValue(
      influencingMasterQuality, FoodBrokerVertexLabels.SALESQUOTATION_VERTEX_LABEL,
      FoodBrokerConfigurationKeys.SQ_LINEQUANTITY_CONFIG_KEY, true);

    properties.set(
      FoodBrokerPropertyKeys.SUPERTYPE_KEY,
      FoodBrokerPropertyValues.SUPERCLASS_VALUE_TRANSACTIONAL
    );
    properties.set(FoodBrokerPropertyKeys.PURCHPRICE_KEY, getPrice(product));
    properties.set(FoodBrokerPropertyKeys.SALESPRICE_KEY,
      salesMargin
        .add(BigDecimal.ONE)
        .multiply(getPrice(product))
        .setScale(2, BigDecimal.ROUND_HALF_UP)
    );
    properties.set(FoodBrokerPropertyKeys.QUANTITY_KEY, quantity);

    return newEdge(label, salesQuotation.getId(), product, properties);
  }


  /**
   * Creates a sales order for a confirmed sales quotation.
   *
   * @param salesQuotation the confirmed quotation
   * @return vertex representation of a sales order
   */
  private Vertex newSalesOrder(Vertex salesQuotation) {
    String label = FoodBrokerVertexLabels.SALESORDER_VERTEX_LABEL;
    Properties properties = new Properties();

    List<Float> influencingMasterQuality = Lists.newArrayList();
    GradoopId employee =
      getEdgeTargetId(FoodBrokerEdgeLabels.SENTBY_EDGE_LABEL, salesQuotation.getId());
    GradoopId customer =
      getEdgeTargetId(FoodBrokerEdgeLabels.SENTTO_EDGE_LABEL, salesQuotation.getId());
    // the additional influence is increased of the two master data objects share the same city
    // or holding
    Float additionalInfluence = getAdditionalInfluence(
      employee, FoodBrokerBroadcastNames.BC_EMPLOYEES,
      customer, FoodBrokerBroadcastNames.BC_CUSTOMERS);

    influencingMasterQuality.add(
      getEdgeTargetQuality(employee, FoodBrokerBroadcastNames.BC_EMPLOYEES) * additionalInfluence);
    influencingMasterQuality.add(
      getEdgeTargetQuality(customer, FoodBrokerBroadcastNames.BC_CUSTOMERS) * additionalInfluence);

    LocalDate salesQuotationDate = salesQuotation
      .getPropertyValue(FoodBrokerPropertyKeys.DATE_KEY)
      .getDate();
    LocalDate date = config.delayDelayConfiguration(salesQuotationDate,
      influencingMasterQuality, FoodBrokerVertexLabels.SALESQUOTATION_VERTEX_LABEL,
      FoodBrokerConfigurationKeys.SQ_CONFIRMATIONDELAY_CONFIG_KEY);
    String bid = createBusinessIdentifier(
      currentId++, FoodBrokerAcronyms.SALESORDER_ACRONYM);
    // get random employee and collect all quality values from influencing master data objects
    influencingMasterQuality.clear();
    employee = getNextEmployee();
    customer = getEdgeTargetId(FoodBrokerEdgeLabels.SENTTO_EDGE_LABEL, salesQuotation.getId());
    // the additional influence is increased of the two master data objects share the same city
    // or holding
    additionalInfluence = getAdditionalInfluence(
      employee, FoodBrokerBroadcastNames.BC_EMPLOYEES,
      customer, FoodBrokerBroadcastNames.BC_CUSTOMERS);

    influencingMasterQuality.add(
      getEdgeTargetQuality(customer, FoodBrokerBroadcastNames.BC_CUSTOMERS) * additionalInfluence);
    influencingMasterQuality.add(getQuality(employeeIndex, employee) * additionalInfluence);

    // set calculated properties
    properties.set(
      FoodBrokerPropertyKeys.SUPERTYPE_KEY,
      FoodBrokerPropertyValues.SUPERCLASS_VALUE_TRANSACTIONAL
    );
    properties.set(FoodBrokerPropertyKeys.DATE_KEY, date);
    properties.set(FoodBrokerPropertyKeys.SOURCEID_KEY, bid);
    properties.set(FoodBrokerPropertyKeys.DELIVERYDATE_KEY, config.delayDelayConfiguration(
      date, influencingMasterQuality, FoodBrokerVertexLabels.SALESORDER_VERTEX_LABEL,
      FoodBrokerConfigurationKeys.SO_DELIVERYAGREEMENTDELAY_CONFIG_KEY));

    Vertex salesOrder = newVertex(label, properties);

    // create all relevant edges
    newEdge(FoodBrokerEdgeLabels.RECEIVEDFROM_EDGE_LABEL, salesOrder.getId(), getEdgeTargetId(
      FoodBrokerEdgeLabels.SENTTO_EDGE_LABEL, salesQuotation.getId()));
    newEdge(FoodBrokerEdgeLabels.PROCESSEDBY_EDGE_LABEL, salesOrder.getId(), employee);
    newEdge(FoodBrokerEdgeLabels.BASEDON_EDGE_LABEL, salesOrder.getId(), salesQuotation.getId());

    return salesOrder;
  }

  /**
   * Creates new sales order lines.
   *
   * @param salesOrder order, corresponding to the lines
   * @param salesQuotationLines lines, corresponding to the quotation
   * @return list of vertices which represent a sales order line
   */
  private List<Edge> newSalesOrderLines(Vertex salesOrder,
    List<Edge> salesQuotationLines) {
    List<Edge> salesOrderLines = Lists.newArrayList();
    Edge salesOrderLine;

    // create one sales orde rline for each sales quotation line
    for (Edge singleSalesQuotationLine : salesQuotationLines) {
      salesOrderLine = newSalesOrderLine(salesOrder, singleSalesQuotationLine);
      salesOrderLines.add(salesOrderLine);
    }

    return  salesOrderLines;
  }

  /**
   * Creaes a new sales order line.
   *
   * @param salesOrder order, corresponding to the lines
   * @param salesQuotationLine lines, corresponding to the quotation
   * @return vertex representation of a sales order line
   */
  private Edge newSalesOrderLine(Vertex salesOrder, Edge salesQuotationLine) {
    String label = FoodBrokerEdgeLabels.SALESORDERLINE_EDGE_LABEL;
    Properties properties = new Properties();

    // set properties based on the sales quotation line
    properties.set(
      FoodBrokerPropertyKeys.SUPERTYPE_KEY,
      FoodBrokerPropertyValues.SUPERCLASS_VALUE_TRANSACTIONAL
    );
    properties.set(FoodBrokerPropertyKeys.SALESPRICE_KEY, salesQuotationLine.getPropertyValue(
      FoodBrokerPropertyKeys.SALESPRICE_KEY).getBigDecimal());
    properties.set(FoodBrokerPropertyKeys.QUANTITY_KEY, salesQuotationLine.getPropertyValue(
      FoodBrokerPropertyKeys.QUANTITY_KEY).getInt());

    return newEdge(label, salesOrder.getId(),
      salesQuotationLine.getTargetId(), properties);
  }

  /**
   * Creates new purch orders.
   *
   * @param salesOrder sales order, corresponding to the new purch order
   * @param salesOrderLines lines, corresponding to the sales order
   * @return list of vertices which represent a purch order
   */
  private List<Vertex> newPurchOrders(Vertex salesOrder, List salesOrderLines) {
    List<Vertex> purchOrders = Lists.newArrayList();
    Vertex purchOrder;

    int numberOfVendors = config.getIntRangeConfigurationValue(
      new ArrayList<>(), FoodBrokerVertexLabels.PURCHORDER_VERTEX_LABEL,
      FoodBrokerConfigurationKeys.PO_NUMBEROFVENDORS_CONFIG_KEY, true);
    for (int i = 0; i < (numberOfVendors > salesOrderLines.size() ?
      salesOrderLines.size() : numberOfVendors); i++) {
      purchOrder = newPurchOrder(salesOrder, getNextEmployee());
      purchOrders.add(purchOrder);
    }

    return purchOrders;
  }

  /**
   * Creates a new purch order.
   *
   * @param salesOrder sales order, corresponding to the new purch order
   * @param processedBy employee chosen for this job
   * @return vertex representation of a purch order
   */
  private Vertex newPurchOrder(Vertex salesOrder, GradoopId processedBy) {
    String label = FoodBrokerVertexLabels.PURCHORDER_VERTEX_LABEL;
    Properties properties = new Properties();

    // calculate and set the properties
    LocalDate salesOrderDate =
      salesOrder.getPropertyValue(FoodBrokerPropertyKeys.DATE_KEY).getDate();
    LocalDate date = config.delayDelayConfiguration(salesOrderDate,
      getEdgeTargetQuality(
        FoodBrokerEdgeLabels.PROCESSEDBY_EDGE_LABEL,
        salesOrder.getId(),
        FoodBrokerBroadcastNames.BC_EMPLOYEES
      ),
      FoodBrokerVertexLabels.PURCHORDER_VERTEX_LABEL,
      FoodBrokerConfigurationKeys.PO_PURCHASEDELAY_CONFIG_KEY
    );
    String bid = createBusinessIdentifier(currentId++, FoodBrokerAcronyms.PURCHORDER_ACRONYM);

    properties.set(
      FoodBrokerPropertyKeys.SUPERTYPE_KEY,
      FoodBrokerPropertyValues.SUPERCLASS_VALUE_TRANSACTIONAL
    );
    properties.set(FoodBrokerPropertyKeys.DATE_KEY, date);
    properties.set(FoodBrokerPropertyKeys.SOURCEID_KEY, bid);

    Vertex purchOrder = newVertex(label, properties);

    // create all relevant edges
    newEdge(FoodBrokerEdgeLabels.SERVES_EDGE_LABEL, purchOrder.getId(), salesOrder.getId());
    newEdge(FoodBrokerEdgeLabels.PLACEDAT_EDGE_LABEL, purchOrder.getId(), getNextVendor());
    newEdge(FoodBrokerEdgeLabels.PROCESSEDBY_EDGE_LABEL, purchOrder.getId(), processedBy);

    return purchOrder;
  }

  /**
   * Creates new purch order lines.
   *
   * @param purchOrders purch orders, corresponding to the new lines
   * @param salesOrderLines sales order lines, corresponding to the sales order
   * @return list of vertices which represent a purch order line
   */
  private List<Edge> newPurchOrderLines(List<Vertex> purchOrders,
    List<Edge> salesOrderLines) {
    List<Edge> purchOrderLines = Lists.newArrayList();
    Edge purchOrderLine;
    Vertex purchOrder;

    int linesPerPurchOrder = salesOrderLines.size() / purchOrders.size();

    // create the correct purch order line, depending on the sales order line, for each purch order
    for (Edge singleSalesOrderLine : salesOrderLines) {
      int purchOrderIndex = salesOrderLines.indexOf(singleSalesOrderLine) /
        linesPerPurchOrder;
      if (purchOrderIndex > (purchOrders.size() - 1)) {
        purchOrderIndex = purchOrders.size() - 1;
      }
      purchOrder = purchOrders.get(purchOrderIndex);
      purchOrderLine = newPurchOrderLine(purchOrder, singleSalesOrderLine);

      purchOrderLines.add(purchOrderLine);
    }

    return purchOrderLines;
  }

  /**
   * Creates a new purch oder line.
   *
   * @param purchOrder purch order, corresponding to the new line
   * @param salesOrderLine sales order line, corresponding to the sales order
   * @return vertex representation of a purch order line
   */
  private Edge newPurchOrderLine(Vertex purchOrder, Edge salesOrderLine) {
    String label = FoodBrokerEdgeLabels.PURCHORDERLINE_EDGE_LABEL;
    Edge purchOrderLine;
    Properties properties = new Properties();

    // calculate and set the properties
    BigDecimal price = getPrice(salesOrderLine.getTargetId());

    List<Float> influencingMasterQuality = Lists.newArrayList();

    GradoopId employee =
      getEdgeTargetId(FoodBrokerEdgeLabels.PROCESSEDBY_EDGE_LABEL, purchOrder.getId());
    GradoopId vendor =
      getEdgeTargetId(FoodBrokerEdgeLabels.PLACEDAT_EDGE_LABEL, purchOrder.getId());
    // the additional influence is increased of the two master data objects share the same city
    // or location
    Float additionalInfluence = getAdditionalInfluence(
      employee, FoodBrokerBroadcastNames.BC_EMPLOYEES, vendor, FoodBrokerBroadcastNames.BC_VENDORS);

    influencingMasterQuality.add(
      getEdgeTargetQuality(employee, FoodBrokerBroadcastNames.BC_EMPLOYEES) * additionalInfluence);
    influencingMasterQuality.add(
      getEdgeTargetQuality(vendor, FoodBrokerBroadcastNames.BC_VENDORS) * additionalInfluence);

    BigDecimal purchPrice = price;
    purchPrice = config.getDecimalVariationConfigurationValue(
      influencingMasterQuality, FoodBrokerVertexLabels.PURCHORDER_VERTEX_LABEL,
      FoodBrokerConfigurationKeys.PO_PRICEVARIATION_CONFIG_KEY, false)
      .add(BigDecimal.ONE)
      .multiply(purchPrice)
      .setScale(2, BigDecimal.ROUND_HALF_UP);

    properties.set(
      FoodBrokerPropertyKeys.SUPERTYPE_KEY,
      FoodBrokerPropertyValues.SUPERCLASS_VALUE_TRANSACTIONAL
    );
    // create indirect connection to the corresponding sales order
    properties.set("salesOrderLine", salesOrderLine.getId().toString());
    properties.set(FoodBrokerPropertyKeys.QUANTITY_KEY,
      salesOrderLine.getPropertyValue(FoodBrokerPropertyKeys.QUANTITY_KEY).getInt());
    properties.set(FoodBrokerPropertyKeys.PURCHPRICE_KEY, purchPrice);

    purchOrderLine = newEdge(label, purchOrder.getId(), salesOrderLine.getTargetId(),
      properties);

    // create indirect connection to the corresponding purch order
    salesOrderLine.setProperty("purchOrderLine", purchOrderLine.getId().toString());

    return purchOrderLine;
  }

  /**
   * Creates new delivery notes.
   *
   * @param purchOrders purch orders, corresponding to the new notes
   * @return list of vertices which represent a delivery note
   */
  private List<Vertex> newDeliveryNotes(List<Vertex> purchOrders) {
    List<Vertex> deliveryNotes = Lists.newArrayList();
    Vertex deliveryNote;
    // create one delivery note for each purch order
    for (Vertex singlePurchOrder : purchOrders) {
      deliveryNote = newDeliveryNote(singlePurchOrder);
      deliveryNotes.add(deliveryNote);
    }

    return deliveryNotes;
  }

  /**
   * Creates a new delivery note.
   *
   * @param purchOrder purch order, corresponding to the new note
   * @return vertex representation of a delivery note
   */
  private Vertex newDeliveryNote(Vertex purchOrder) {
    String label = FoodBrokerVertexLabels.DELIVERYNOTE_VERTEX_LABEL;
    Properties properties = new Properties();

    // calculate and set the properties
    LocalDate purchOrderDate = purchOrder
      .getPropertyValue(FoodBrokerPropertyKeys.DATE_KEY).getDate();
    GradoopId operatedBy = getNextLogistic();

    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getQuality(logisticIndex, operatedBy));
    influencingMasterQuality.add(getEdgeTargetQuality(
      FoodBrokerEdgeLabels.PLACEDAT_EDGE_LABEL,
      purchOrder.getId(),
      FoodBrokerBroadcastNames.BC_VENDORS)
    );

    LocalDate date = config.delayDelayConfiguration(
      purchOrderDate, influencingMasterQuality, FoodBrokerVertexLabels.PURCHORDER_VERTEX_LABEL,
      FoodBrokerConfigurationKeys.PO_DELIVERYDELAY_CONFIG_KEY);
    String bid = createBusinessIdentifier(currentId++, FoodBrokerAcronyms.DELIVERYNOTE_ACRONYM);

    properties.set(
      FoodBrokerPropertyKeys.SUPERTYPE_KEY,
      FoodBrokerPropertyValues.SUPERCLASS_VALUE_TRANSACTIONAL
    );
    properties.set(FoodBrokerPropertyKeys.DATE_KEY, date);
    properties.set(FoodBrokerPropertyKeys.SOURCEID_KEY, bid);
    properties.set("trackingCode", "***TODO***");

    Vertex deliveryNote = newVertex(label, properties);

    // create all relevant edges
    newEdge(FoodBrokerEdgeLabels.CONTAINS_EDGE_LABEL, deliveryNote.getId(), purchOrder.getId());
    newEdge(FoodBrokerEdgeLabels.OPERATEDBY_EDGE_LABEL, deliveryNote.getId(), operatedBy);

    return deliveryNote;
  }

  /**
   * Creates new purch invoices.
   *
   * @param purchOrderLines purch order lines, corresponding to the new
   *                        invoices
   * @return list of vertices which represent a purch invoice
   */
  private List<Vertex> newPurchInvoices(List<Edge> purchOrderLines) {
    Vertex purchOrder;
    Map<Vertex, BigDecimal> purchOrderTotals = Maps.newHashMap();

    BigDecimal total;
    BigDecimal purchAmount;
    // calculate to total cost for each purch order
    for (Edge purchOrderLine : purchOrderLines) {
      purchOrder = vertexMap.get(purchOrderLine.getSourceId());
      total = BigDecimal.ZERO;

      if (purchOrderTotals.containsKey(purchOrder)) {
        total = purchOrderTotals.get(purchOrder);
      }
      purchAmount = BigDecimal.valueOf(
        purchOrderLine.getPropertyValue(FoodBrokerPropertyKeys.QUANTITY_KEY).getInt());
      purchAmount = purchAmount.multiply(purchOrderLine.getPropertyValue(
        FoodBrokerPropertyKeys.PURCHPRICE_KEY).getBigDecimal());
      total = total.add(purchAmount);
      purchOrderTotals.put(purchOrder, total);
    }

    List<Vertex> purchInvoices = Lists.newArrayList();

    // create one invoice for each purch order
    for (Map.Entry<Vertex, BigDecimal> purchOrderTotal : purchOrderTotals.entrySet()) {
      purchInvoices.add(newPurchInvoice(
        purchOrderTotal.getKey(),
        purchOrderTotal.getValue()
      ));
    }

    return purchInvoices;
  }

  /**
   * Creates a new purch invoice.
   *
   * @param purchOrder purch order, corresponding to the new purch invoice
   * @param total total purch amount
   * @return vertex representation of a purch invoice
   */
  private Vertex newPurchInvoice(Vertex purchOrder, BigDecimal total) {
    String label = FoodBrokerVertexLabels.PURCHINVOICE_VERTEX_LABEL;
    Properties properties = new Properties();

    // calculate and set the properties
    LocalDate purchOrderDate =
      purchOrder.getPropertyValue(FoodBrokerPropertyKeys.DATE_KEY).getDate();
    LocalDate date = config.delayDelayConfiguration(purchOrderDate,
      getEdgeTargetQuality(
        FoodBrokerEdgeLabels.PLACEDAT_EDGE_LABEL,
        purchOrder.getId(),
        FoodBrokerBroadcastNames.BC_VENDORS
      ),
      FoodBrokerVertexLabels.PURCHORDER_VERTEX_LABEL,
      FoodBrokerConfigurationKeys.PO_INVOICEDELAY_CONFIG_KEY
    );

    properties.set(
      FoodBrokerPropertyKeys.SUPERTYPE_KEY,
      FoodBrokerPropertyValues.SUPERCLASS_VALUE_TRANSACTIONAL
    );
    properties.set(FoodBrokerPropertyKeys.DATE_KEY, date);
    String bid = createBusinessIdentifier(currentId++, FoodBrokerAcronyms.PURCHINVOICE_ACRONYM);
    properties.set(FoodBrokerPropertyKeys.SOURCEID_KEY, bid);
    properties.set(FoodBrokerPropertyKeys.EXPENSE_KEY, total);
    properties.set("text", "*** TODO @ Brokerage ***");

    Vertex purchInvoice = newVertex(label, properties);

    // create relevant edge
    newEdge(FoodBrokerEdgeLabels.CREATEDFOR_EDGE_LABEL, purchInvoice.getId(), purchOrder.getId());

    return purchInvoice;
  }

  /**
   * Creates a new sales invoice.
   *
   * @param salesOrderLines sales order line, corresponding to the new invoice
   * @return vertex representation of a sales invoice
   */
  private Vertex newSalesInvoice(List<Edge> salesOrderLines) {
    String label = FoodBrokerVertexLabels.SALESINVOICE_VERTEX_LABEL;
    Vertex salesOrder = vertexMap.get(salesOrderLines.get(0).getSourceId());
    Properties properties = new Properties();

    // calculate and set the properties
    LocalDate salesOrderDate =
      salesOrder.getPropertyValue(FoodBrokerPropertyKeys.DATE_KEY).getDate();
    LocalDate date = config.delayDelayConfiguration(salesOrderDate,
      getEdgeTargetQuality(
        FoodBrokerEdgeLabels.PROCESSEDBY_EDGE_LABEL,
        salesOrder.getId(),
        FoodBrokerBroadcastNames.BC_EMPLOYEES
      ),
      FoodBrokerVertexLabels.SALESORDER_VERTEX_LABEL,
      FoodBrokerConfigurationKeys.SO_INVOICEDELAY_CONFIG_KEY
    );

    properties.set(
      FoodBrokerPropertyKeys.SUPERTYPE_KEY,
      FoodBrokerPropertyValues.SUPERCLASS_VALUE_TRANSACTIONAL
    );
    properties.set(FoodBrokerPropertyKeys.DATE_KEY, date);
    String bid = createBusinessIdentifier(currentId++, FoodBrokerAcronyms.SALESINVOICE_ACRONYM);
    properties.set(FoodBrokerPropertyKeys.SOURCEID_KEY, bid);
    properties.set(FoodBrokerPropertyKeys.REVENUE_KEY, BigDecimal.ZERO);
    properties.set("text", "*** TODO @ Brokerage ***");

    Vertex salesInvoice = newVertex(label, properties);

    BigDecimal revenue;
    BigDecimal salesAmount;
    // set the invoices revenue considering all sales order lines
    for (Edge salesOrderLine : salesOrderLines) {
      salesAmount = BigDecimal.valueOf(salesOrderLine.getPropertyValue(
        FoodBrokerPropertyKeys.QUANTITY_KEY).getInt())
        .multiply(
          salesOrderLine.getPropertyValue(FoodBrokerPropertyKeys.SALESPRICE_KEY).getBigDecimal()
        )
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      revenue = salesInvoice.getPropertyValue(FoodBrokerPropertyKeys.REVENUE_KEY).getBigDecimal();
      revenue = revenue.add(salesAmount);
      salesInvoice.setProperty(FoodBrokerPropertyKeys.REVENUE_KEY, revenue);
    }

    // create relevant edge
    newEdge(FoodBrokerEdgeLabels.CREATEDFOR_EDGE_LABEL, salesInvoice.getId(), salesOrder.getId());

    return salesInvoice;
  }
}
