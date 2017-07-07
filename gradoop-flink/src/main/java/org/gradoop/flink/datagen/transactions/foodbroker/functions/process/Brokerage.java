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
import org.apache.flink.api.common.functions.MapFunction;
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
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConstants;
import org.gradoop.flink.representation.transactional.GraphTransaction;


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
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    productPriceMap = getRuntimeContext().<Map<GradoopId, BigDecimal>>
      getBroadcastVariable(FoodBrokerConstants.PRODUCT_PRICE_MAP_BC).get(0);

    productPriceIterator = productPriceMap.entrySet().iterator();
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
    GradoopId employee = getEdgeTargetId(FoodBrokerConstants.SENTBY_EDGE_LABEL, salesQuotation.getId());
    GradoopId customer = getEdgeTargetId(FoodBrokerConstants.SENTTO_EDGE_LABEL, salesQuotation.getId());
    // the additional influence is increased of the two master data objects share the same city
    // or holding
    Float additionalInfluence = getAdditionalInfluence(
      employee, FoodBrokerConstants.BC_EMPLOYEES, customer, FoodBrokerConstants.CUSTOMER_MAP_BC);

    influencingMasterQuality.add(
      getEdgeTargetQuality(employee, FoodBrokerConstants.BC_EMPLOYEES) * additionalInfluence);
    influencingMasterQuality.add(
      getEdgeTargetQuality(customer, FoodBrokerConstants.CUSTOMER_MAP_BC) * additionalInfluence);

    return config.happensTransitionConfiguration(
      influencingMasterQuality, FoodBrokerConstants.SALESQUOTATION_VERTEX_LABEL,
      FoodBrokerConstants.SQ_CONFIRMATIONPROBABILITY_CONFIG_KEY, false);
  }

  /**
   * Creates a new sales quotation.
   *
   * @param startDate of the quotation
   * @return vertex representation of a sales quotation
   */
  private Vertex newSalesQuotation(LocalDate startDate) {
    String label = FoodBrokerConstants.SALESQUOTATION_VERTEX_LABEL;
    Properties properties = new Properties();

    String bid = createBusinessIdentifier(currentId++, FoodBrokerConstants.SALESQUOTATION_ACRONYM);

    // set properties
    properties.set(FoodBrokerConstants.SUPERTYPE_KEY, FoodBrokerConstants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set(FoodBrokerConstants.DATE_KEY, startDate);
    properties.set(FoodBrokerConstants.SOURCEID_KEY, bid);

    Vertex salesQuotation = newVertex(label, properties);

    // select random employee and customer
    GradoopId rndEmployee = getNextEmployee();
    GradoopId rndCustomer = getNextCustomer();

    newEdge(FoodBrokerConstants.SENTBY_EDGE_LABEL, salesQuotation.getId(), rndEmployee);
    newEdge(FoodBrokerConstants.SENTTO_EDGE_LABEL, salesQuotation.getId(), rndCustomer);

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
    GradoopId employee = getEdgeTargetId(FoodBrokerConstants.SENTBY_EDGE_LABEL, salesQuotation.getId());
    GradoopId customer = getEdgeTargetId(FoodBrokerConstants.SENTTO_EDGE_LABEL, salesQuotation.getId());
    // the additional influence is increased of the two master data objects share the same city
    // or holding
    Float additionalInfluence = getAdditionalInfluence(
      employee, FoodBrokerConstants.BC_EMPLOYEES, customer, FoodBrokerConstants.CUSTOMER_MAP_BC);

    influencingMasterQuality.add(
      getEdgeTargetQuality(employee, FoodBrokerConstants.BC_EMPLOYEES) * additionalInfluence);
    influencingMasterQuality.add(
      getEdgeTargetQuality(customer, Constants.CUSTOMER_MAP_BC) * additionalInfluence);

    int numberOfQuotationLines = config.getIntRangeConfigurationValue(
      influencingMasterQuality, FoodBrokerConstants.SALESQUOTATION_VERTEX_LABEL,
      FoodBrokerConstants.SQ_LINES_CONFIG_KEY, true);

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
    String label = FoodBrokerConstants.SALESQUOTATIONLINE_EDGE_LABEL;
    Properties properties = new Properties();

    List<Float> influencingMasterQuality = Lists.newArrayList();
    GradoopId employee = getEdgeTargetId(FoodBrokerConstants.SENTBY_EDGE_LABEL, salesQuotation.getId());
    GradoopId customer = getEdgeTargetId(FoodBrokerConstants.SENTTO_EDGE_LABEL, salesQuotation.getId());
    // the additional influence is increased of the two master data objects share the same city
    // or holding
    Float additionalInfluence = getAdditionalInfluence(
      employee, FoodBrokerConstants.BC_EMPLOYEES, customer, FoodBrokerConstants.CUSTOMER_MAP_BC);

    influencingMasterQuality.add(
      getEdgeTargetQuality(employee, FoodBrokerConstants.BC_EMPLOYEES) * additionalInfluence);
    influencingMasterQuality.add(
      getEdgeTargetQuality(customer, FoodBrokerConstants.CUSTOMER_MAP_BC) * additionalInfluence);
    influencingMasterQuality.add(productQualityMap.get(product));

    // calculate and set the lines properties
    BigDecimal salesMargin = config.getDecimalVariationConfigurationValue(
      influencingMasterQuality, FoodBrokerConstants.SALESQUOTATION_VERTEX_LABEL,
      FoodBrokerConstants.SQ_SALESMARGIN_CONFIG_KEY, true);

    influencingMasterQuality.clear();
    int quantity = config.getIntRangeConfigurationValue(
      influencingMasterQuality, FoodBrokerConstants.SALESQUOTATION_VERTEX_LABEL,
      FoodBrokerConstants.SQ_LINEQUANTITY_CONFIG_KEY, true);

    properties.set(FoodBrokerConstants.SUPERTYPE_KEY, FoodBrokerConstants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set(FoodBrokerConstants.PURCHPRICE_KEY, productPriceMap.get(product));
    properties.set(FoodBrokerConstants.SALESPRICE_KEY,
      salesMargin
        .add(BigDecimal.ONE)
        .multiply(productPriceMap.get(product))
        .setScale(2, BigDecimal.ROUND_HALF_UP)
    );
    properties.set(FoodBrokerConstants.QUANTITY_KEY, quantity);

    return newEdge(label, salesQuotation.getId(), product, properties);
  }

  /**
   * Creates a sales order for a confirmed sales quotation.
   *
   * @param salesQuotation the confirmed quotation
   * @return vertex representation of a sales order
   */
  private Vertex newSalesOrder(Vertex salesQuotation) {
    String label = FoodBrokerConstants.SALESORDER_VERTEX_LABEL;
    Properties properties = new Properties();

    List<Float> influencingMasterQuality = Lists.newArrayList();
    GradoopId employee = getEdgeTargetId(FoodBrokerConstants.SENTBY_EDGE_LABEL, salesQuotation.getId());
    GradoopId customer = getEdgeTargetId(FoodBrokerConstants.SENTTO_EDGE_LABEL, salesQuotation.getId());
    // the additional influence is increased of the two master data objects share the same city
    // or holding
    Float additionalInfluence = getAdditionalInfluence(
      employee, FoodBrokerConstants.BC_EMPLOYEES, customer, FoodBrokerConstants.CUSTOMER_MAP_BC);

    influencingMasterQuality.add(
      getEdgeTargetQuality(employee, FoodBrokerConstants.BC_EMPLOYEES) * additionalInfluence);
    influencingMasterQuality.add(
      getEdgeTargetQuality(customer, FoodBrokerConstants.CUSTOMER_MAP_BC) * additionalInfluence);

    LocalDate salesQuotationDate = salesQuotation
      .getPropertyValue(FoodBrokerConstants.DATE_KEY)
      .getDate();
    LocalDate date = config.delayDelayConfiguration(salesQuotationDate,
      influencingMasterQuality, FoodBrokerConstants.SALESQUOTATION_VERTEX_LABEL,
      FoodBrokerConstants.SQ_CONFIRMATIONDELAY_CONFIG_KEY);
    String bid = createBusinessIdentifier(
      currentId++, FoodBrokerConstants.SALESORDER_ACRONYM);
    // get random employee and collect all quality values from influencing master data objects
    influencingMasterQuality.clear();
    employee = getNextEmployee();
    customer = getEdgeTargetId(FoodBrokerConstants.SENTTO_EDGE_LABEL, salesQuotation.getId());
    // the additional influence is increased of the two master data objects share the same city
    // or holding
    additionalInfluence = getAdditionalInfluence(
      employee, FoodBrokerConstants.BC_EMPLOYEES, customer, FoodBrokerConstants.CUSTOMER_MAP_BC);

    influencingMasterQuality.add(
      getEdgeTargetQuality(customer, FoodBrokerConstants.CUSTOMER_MAP_BC) * additionalInfluence);
    influencingMasterQuality.add(getQuality(employeeIndex, employee) * additionalInfluence);

    // set calculated properties
    properties.set(FoodBrokerConstants.SUPERTYPE_KEY, FoodBrokerConstants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set(FoodBrokerConstants.DATE_KEY, date);
    properties.set(FoodBrokerConstants.SOURCEID_KEY, bid);
    properties.set(FoodBrokerConstants.DELIVERYDATE_KEY, config.delayDelayConfiguration(
      date, influencingMasterQuality, FoodBrokerConstants.SALESORDER_VERTEX_LABEL,
      FoodBrokerConstants.SO_DELIVERYAGREEMENTDELAY_CONFIG_KEY));

    Vertex salesOrder = newVertex(label, properties);

    // create all relevant edges
    newEdge(FoodBrokerConstants.RECEIVEDFROM_EDGE_LABEL, salesOrder.getId(), getEdgeTargetId(
      FoodBrokerConstants.SENTTO_EDGE_LABEL, salesQuotation.getId()));
    newEdge(FoodBrokerConstants.PROCESSEDBY_EDGE_LABEL, salesOrder.getId(), employee);
    newEdge(FoodBrokerConstants.BASEDON_EDGE_LABEL, salesOrder.getId(), salesQuotation.getId());

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
    String label = FoodBrokerConstants.SALESORDERLINE_EDGE_LABEL;
    Properties properties = new Properties();

    // set properties based on the sales quotation line
    properties.set(FoodBrokerConstants.SUPERTYPE_KEY, FoodBrokerConstants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set(FoodBrokerConstants.SALESPRICE_KEY, salesQuotationLine.getPropertyValue(
      FoodBrokerConstants.SALESPRICE_KEY).getBigDecimal());
    properties.set(FoodBrokerConstants.QUANTITY_KEY, salesQuotationLine.getPropertyValue(
      FoodBrokerConstants.QUANTITY_KEY).getInt());

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
      new ArrayList<>(), FoodBrokerConstants.PURCHORDER_VERTEX_LABEL,
      FoodBrokerConstants.PO_NUMBEROFVENDORS_CONFIG_KEY, true);
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
    String label = FoodBrokerConstants.PURCHORDER_VERTEX_LABEL;
    Properties properties = new Properties();

    // calculate and set the properties
    LocalDate salesOrderDate = salesOrder.getPropertyValue(FoodBrokerConstants.DATE_KEY).getDate();
    LocalDate date = config.delayDelayConfiguration(salesOrderDate,
      getEdgeTargetQuality(
        FoodBrokerConstants.PROCESSEDBY_EDGE_LABEL, salesOrder.getId(), FoodBrokerConstants.BC_EMPLOYEES),
        FoodBrokerConstants.PURCHORDER_VERTEX_LABEL, FoodBrokerConstants.PO_PURCHASEDELAY_CONFIG_KEY);
    String bid = createBusinessIdentifier(currentId++, FoodBrokerConstants.PURCHORDER_ACRONYM);

    properties.set(FoodBrokerConstants.SUPERTYPE_KEY, FoodBrokerConstants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set(FoodBrokerConstants.DATE_KEY, date);
    properties.set(FoodBrokerConstants.SOURCEID_KEY, bid);

    Vertex purchOrder = newVertex(label, properties);

    // create all relevant edges
    newEdge(FoodBrokerConstants.SERVES_EDGE_LABEL, purchOrder.getId(), salesOrder.getId());
    newEdge(FoodBrokerConstants.PLACEDAT_EDGE_LABEL, purchOrder.getId(), getNextVendor());
    newEdge(FoodBrokerConstants.PROCESSEDBY_EDGE_LABEL, purchOrder.getId(), processedBy);

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
    String label = FoodBrokerConstants.PURCHORDERLINE_EDGE_LABEL;
    Edge purchOrderLine;
    Properties properties = new Properties();

    // calculate and set the properties
    BigDecimal price = productPriceMap.get(salesOrderLine.getTargetId());

    List<Float> influencingMasterQuality = Lists.newArrayList();

    GradoopId employee = getEdgeTargetId(FoodBrokerConstants.PROCESSEDBY_EDGE_LABEL, purchOrder.getId());
    GradoopId vendor = getEdgeTargetId(FoodBrokerConstants.PLACEDAT_EDGE_LABEL, purchOrder.getId());
    // the additional influence is increased of the two master data objects share the same city
    // or location
    Float additionalInfluence = getAdditionalInfluence(
      employee, FoodBrokerConstants.BC_EMPLOYEES, vendor, FoodBrokerConstants.VENDOR_MAP_BC);

    influencingMasterQuality.add(
      getEdgeTargetQuality(employee, FoodBrokerConstants.BC_EMPLOYEES) * additionalInfluence);
    influencingMasterQuality.add(
      getEdgeTargetQuality(vendor, FoodBrokerConstants.VENDOR_MAP_BC) * additionalInfluence);

    BigDecimal purchPrice = price;
    purchPrice = config.getDecimalVariationConfigurationValue(
      influencingMasterQuality, FoodBrokerConstants.PURCHORDER_VERTEX_LABEL,
      FoodBrokerConstants.PO_PRICEVARIATION_CONFIG_KEY, false)
      .add(BigDecimal.ONE)
      .multiply(purchPrice)
      .setScale(2, BigDecimal.ROUND_HALF_UP);

    properties.set(FoodBrokerConstants.SUPERTYPE_KEY, FoodBrokerConstants.SUPERCLASS_VALUE_TRANSACTIONAL);
    // create indirect connection to the corresponding sales order
    properties.set("salesOrderLine", salesOrderLine.getId().toString());
    properties.set(FoodBrokerConstants.QUANTITY_KEY,
      salesOrderLine.getPropertyValue(FoodBrokerConstants.QUANTITY_KEY).getInt());
    properties.set(FoodBrokerConstants.PURCHPRICE_KEY, purchPrice);

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
    String label = FoodBrokerConstants.DELIVERYNOTE_VERTEX_LABEL;
    Properties properties = new Properties();

    // calculate and set the properties
    LocalDate purchOrderDate = purchOrder.getPropertyValue(FoodBrokerConstants.DATE_KEY).getDate();
    GradoopId operatedBy = getNextLogistic();

    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(logisticMap.get(operatedBy));
    influencingMasterQuality.add(getEdgeTargetQuality(
      FoodBrokerConstants.PLACEDAT_EDGE_LABEL, purchOrder.getId(), FoodBrokerConstants.VENDOR_MAP_BC));

    LocalDate date = config.delayDelayConfiguration(
      purchOrderDate, influencingMasterQuality, FoodBrokerConstants.PURCHORDER_VERTEX_LABEL,
      FoodBrokerConstants.PO_DELIVERYDELAY_CONFIG_KEY);
    String bid = createBusinessIdentifier(currentId++, FoodBrokerConstants.DELIVERYNOTE_ACRONYM);

    properties.set(FoodBrokerConstants.SUPERTYPE_KEY, FoodBrokerConstants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set(FoodBrokerConstants.DATE_KEY, date);
    properties.set(FoodBrokerConstants.SOURCEID_KEY, bid);
    properties.set("trackingCode", "***TODO***");

    Vertex deliveryNote = newVertex(label, properties);

    // create all relevant edges
    newEdge(FoodBrokerConstants.CONTAINS_EDGE_LABEL, deliveryNote.getId(), purchOrder.getId());
    newEdge(FoodBrokerConstants.OPERATEDBY_EDGE_LABEL, deliveryNote.getId(), operatedBy);

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
        purchOrderLine.getPropertyValue(FoodBrokerConstants.QUANTITY_KEY).getInt());
      purchAmount = purchAmount.multiply(purchOrderLine.getPropertyValue(
        FoodBrokerConstants.PURCHPRICE_KEY).getBigDecimal());
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
    String label = FoodBrokerConstants.PURCHINVOICE_VERTEX_LABEL;
    Properties properties = new Properties();

    // calculate and set the properties
    LocalDate purchOrderDate = purchOrder.getPropertyValue(FoodBrokerConstants.DATE_KEY).getDate();
    LocalDate date = config.delayDelayConfiguration(purchOrderDate,
      getEdgeTargetQuality(
        FoodBrokerConstants.PLACEDAT_EDGE_LABEL, purchOrder.getId(), FoodBrokerConstants.VENDOR_MAP_BC),
      FoodBrokerConstants.PURCHORDER_VERTEX_LABEL, FoodBrokerConstants.PO_INVOICEDELAY_CONFIG_KEY);

    properties.set(FoodBrokerConstants.SUPERTYPE_KEY, FoodBrokerConstants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set(FoodBrokerConstants.DATE_KEY, date);
    String bid = createBusinessIdentifier(currentId++, FoodBrokerConstants.PURCHINVOICE_ACRONYM);
    properties.set(FoodBrokerConstants.SOURCEID_KEY, bid);
    properties.set(FoodBrokerConstants.EXPENSE_KEY, total);
    properties.set("text", "*** TODO @ Brokerage ***");

    Vertex purchInvoice = newVertex(label, properties);

    // create relevant edge
    newEdge(FoodBrokerConstants.CREATEDFOR_EDGE_LABEL, purchInvoice.getId(), purchOrder.getId());

    return purchInvoice;
  }

  /**
   * Creates a new sales invoice.
   *
   * @param salesOrderLines sales order line, corresponding to the new invoice
   * @return vertex representation of a sales invoice
   */
  private Vertex newSalesInvoice(List<Edge> salesOrderLines) {
    String label = FoodBrokerConstants.SALESINVOICE_VERTEX_LABEL;
    Vertex salesOrder = vertexMap.get(salesOrderLines.get(0).getSourceId());
    Properties properties = new Properties();

    // calculate and set the properties
    LocalDate salesOrderDate = salesOrder.getPropertyValue(FoodBrokerConstants.DATE_KEY).getDate();
    LocalDate date = config.delayDelayConfiguration(salesOrderDate,
      getEdgeTargetQuality
        (FoodBrokerConstants.PROCESSEDBY_EDGE_LABEL, salesOrder.getId(), FoodBrokerConstants.BC_EMPLOYEES),
      FoodBrokerConstants.SALESORDER_VERTEX_LABEL, FoodBrokerConstants.SO_INVOICEDELAY_CONFIG_KEY);

    properties.set(FoodBrokerConstants.SUPERTYPE_KEY, FoodBrokerConstants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set(FoodBrokerConstants.DATE_KEY, date);
    String bid = createBusinessIdentifier(currentId++, FoodBrokerConstants.SALESINVOICE_ACRONYM);
    properties.set(FoodBrokerConstants.SOURCEID_KEY, bid);
    properties.set(FoodBrokerConstants.REVENUE_KEY, BigDecimal.ZERO);
    properties.set("text", "*** TODO @ Brokerage ***");

    Vertex salesInvoice = newVertex(label, properties);

    BigDecimal revenue;
    BigDecimal salesAmount;
    // set the invoices revenue considering all sales order lines
    for (Edge salesOrderLine : salesOrderLines) {
      salesAmount = BigDecimal.valueOf(salesOrderLine.getPropertyValue(
        FoodBrokerConstants.QUANTITY_KEY).getInt())
        .multiply(salesOrderLine.getPropertyValue(FoodBrokerConstants.SALESPRICE_KEY).getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      revenue = salesInvoice.getPropertyValue(FoodBrokerConstants.REVENUE_KEY).getBigDecimal();
      revenue = revenue.add(salesAmount);
      salesInvoice.setProperty(FoodBrokerConstants.REVENUE_KEY, revenue);
    }

    // create relevant edge
    newEdge(FoodBrokerConstants.CREATEDFOR_EDGE_LABEL, salesInvoice.getId(), salesOrder.getId());

    return salesInvoice;
  }
}
