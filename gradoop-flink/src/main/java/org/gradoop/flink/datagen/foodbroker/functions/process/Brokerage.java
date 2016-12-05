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

package org.gradoop.flink.datagen.foodbroker.functions.process;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.representation.transactional.GraphTransaction;


import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Map partition function which spreads the whole brokerage process equally to each worker.
 */
public class Brokerage
  extends AbstractProcess
  implements MapPartitionFunction<Long, GraphTransaction> {

  /**
   * Valued constructor
   *
   * @param graphHeadFactory EPGM graph head facroty
   * @param vertexFactory EPGM vertex factory
   * @param edgeFactory EPGM edge factory
   * @param config Foodbroker configuration
   */
  public Brokerage(GraphHeadFactory graphHeadFactory,
    VertexFactory vertexFactory, EdgeFactory edgeFactory,
    FoodBrokerConfig config) {
    super(graphHeadFactory, vertexFactory, edgeFactory, config);
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    productPriceMap = getRuntimeContext().<Map<GradoopId, BigDecimal>>
      getBroadcastVariable(Constants.PRODUCT_PRICE_MAP).get(0);

    productPriceIterator = productPriceMap.entrySet().iterator();
  }

  @Override
  public void mapPartition(Iterable<Long> iterable, Collector<GraphTransaction> collector)
    throws Exception {
    GraphHead graphHead;
    GraphTransaction graphTransaction;

    long startDate = config.getStartDate();

    for (Long seed: iterable) {
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
        List<Vertex> deliveryNotes = newDeliveryNotes(purchOrders);

        // PurchInvoices
        List<Vertex> purchInvoices = newPurchInvoices(purchOrderLines);

        // SalesInvoices
        Vertex salesInvoice = newSalesInvoice(salesOrderLines);
      }
      graphTransaction.setGraphHead(graphHead);
      graphTransaction.setVertices(getVertices());
      graphTransaction.setEdges(getEdges());
      collector.collect(graphTransaction);
    }
  }

  /**
   * Checks if a sales quotation is confirmed.
   *
   * @param salesQuotation the quotation to be checked
   * @return true, if quotation is confirmed
   */
  private boolean confirmed(Vertex salesQuotation) {
    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentBy", salesQuotation.getId(), Constants.EMPLOYEE_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentTo", salesQuotation.getId(), Constants.CUSTOMER_MAP));

    return config.happensTransitionConfiguration(influencingMasterQuality,
      "SalesQuotation", "confirmationProbability");
  }

  /**
   * Creates a new sales quotation.
   *
   * @param startDate of the quotation
   * @return vertex representation of a sales quotation
   */
  private Vertex newSalesQuotation(long startDate) {
    String label = "SalesQuotation";
    Properties properties = new Properties();

    String bid = createBusinessIdentifier(currentId++, Constants.SALESQUOTATION_ACRONYM);

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("date", startDate);
    properties.set("num", bid);

    Vertex salesQuotation = newVertex(label, properties);

    GradoopId rndEmployee = getNextEmployee();
    GradoopId rndCustomer = getNextCustomer();

    newEdge("sentBy", salesQuotation.getId(), rndEmployee);
    newEdge("sentTo", salesQuotation.getId(), rndCustomer);

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
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentBy", salesQuotation.getId(), Constants.EMPLOYEE_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentTo", salesQuotation.getId(), Constants.CUSTOMER_MAP));

    int numberOfQuotationLines = config.getIntRangeConfigurationValue(
      influencingMasterQuality, "SalesQuotation", "lines");

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
    String label = "SalesQuotationLine";
    Properties properties = new Properties();

    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentBy", salesQuotation.getId(), Constants.EMPLOYEE_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentTo", salesQuotation.getId(), Constants.CUSTOMER_MAP));
    influencingMasterQuality.add(productQualityMap.get(product));

    BigDecimal salesMargin = config.getDecimalVariationConfigurationValue(
      influencingMasterQuality, "SalesQuotation", "salesMargin");

    int quantity = config.getIntRangeConfigurationValue(
      new ArrayList<Float>(), "SalesQuotation", "lineQuantity");

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("purchPrice", productPriceMap.get(product));
    properties.set("salesPrice",
      salesMargin
        .add(BigDecimal.ONE)
        .multiply(productPriceMap.get(product))
        .setScale(2, BigDecimal.ROUND_HALF_UP)
    );
    properties.set("quantity", quantity);

    return newEdge(label, salesQuotation.getId(), product, properties);
  }

  /**
   * Creates a sales order for a confirmed sales quotation.
   *
   * @param salesQuotation the confirmed quotation
   * @return vertex representation of a sales order
   */
  private Vertex newSalesOrder(Vertex salesQuotation) {
    String label = "SalesOrder";
    Properties properties = new Properties();

    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentBy", salesQuotation.getId(), Constants.EMPLOYEE_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentTo", salesQuotation.getId(), Constants.CUSTOMER_MAP));

    Long salesQuotationDate = salesQuotation
      .getPropertyValue("date")
      .getLong();
    long date = config.delayDelayConfiguration(salesQuotationDate,
      influencingMasterQuality, "SalesQuotation", "confirmationDelay");
    String bid = createBusinessIdentifier(
      currentId++, Constants.SALESORDER_ACRONYM);
    GradoopId rndEmployee = getNextEmployee();
    influencingMasterQuality.clear();
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentTo", salesQuotation.getId(), Constants.CUSTOMER_MAP));
    influencingMasterQuality.add(employeeMap.get(rndEmployee));

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("date", date);
    properties.set("num", bid);
    properties.set("deliveryDate", config.delayDelayConfiguration(date,
      influencingMasterQuality, "SalesOrder", "deliveryAgreementDelay"));

    Vertex salesOrder = newVertex(label, properties);

    newEdge("receivedFrom", salesOrder.getId(), getEdgeTargetId(
      "sentTo", salesQuotation.getId()));
    newEdge("processedBy", salesOrder.getId(), rndEmployee);
    newEdge("basedOn", salesOrder.getId(), salesQuotation.getId());

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
    String label = "SalesOrderLine";
    Properties properties = new Properties();

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("salesPrice", salesQuotationLine.getPropertyValue(
      "salesPrice").getBigDecimal());
    properties.set("quantity", salesQuotationLine.getPropertyValue(
      "quantity").getInt());

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
      new ArrayList<Float>(), "PurchOrder", "numberOfVendors");
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
    String label = "PurchOrder";
    Properties properties = new Properties();

    long salesOrderDate = salesOrder.getPropertyValue("date")
        .getLong();
    long date = config.delayDelayConfiguration(salesOrderDate,
      getEdgeTargetQuality("processedBy", salesOrder.getId(),
        Constants.EMPLOYEE_MAP), "PurchOrder", "purchaseDelay");
    String bid = createBusinessIdentifier(
      currentId++, Constants.PURCHORDER_ACRONYM);

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("date", date);
    properties.set("num", bid);

    Vertex purchOrder = newVertex(label, properties);

    newEdge("serves", purchOrder.getId(), salesOrder.getId());
    newEdge("placedAt", purchOrder.getId(), getNextVendor());
    newEdge("processedBy", purchOrder.getId(), processedBy);

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
    String label = "PurchOrderLine";
    Edge purchOrderLine;
    Properties properties = new Properties();

    BigDecimal price = productPriceMap.get(salesOrderLine.getTargetId());

    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality(
      "processedBy", purchOrder.getId(), Constants.EMPLOYEE_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(
      "placedAt", purchOrder.getId(), Constants.VENDOR_MAP));

    BigDecimal purchPrice = price;
    purchPrice = config.getDecimalVariationConfigurationValue(
      influencingMasterQuality, "PurchOrder", "priceVariation")
      .add(BigDecimal.ONE)
      .multiply(purchPrice)
      .setScale(2, BigDecimal.ROUND_HALF_UP);

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("salesOrderLine", salesOrderLine.getId().toString());
    properties.set("quantity", salesOrderLine.getPropertyValue("quantity").getInt());
    properties.set("purchPrice", purchPrice);

    purchOrderLine = newEdge(label, purchOrder.getId(), salesOrderLine.getTargetId(),
      properties);

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
    String label = "DeliveryNote";
    Properties properties = new Properties();

    long purchOrderDate = purchOrder.getPropertyValue("date").getLong();
    GradoopId operatedBy = getNextLogistic();

    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(logisticMap.get(operatedBy));
    influencingMasterQuality.add(getEdgeTargetQuality(
      "placedAt", purchOrder.getId(), Constants.VENDOR_MAP));

    long date = config.delayDelayConfiguration(purchOrderDate,
      influencingMasterQuality, "PurchOrder", "deliveryDelay");
    String bid = createBusinessIdentifier(currentId++, Constants.DELIVERYNOTE_ACRONYM);

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("date", date);
    properties.set("num", bid);
    properties.set("trackingCode", "***TODO***");

    Vertex deliveryNote = newVertex(label, properties);

    newEdge("contains", deliveryNote.getId(), purchOrder.getId());
    newEdge("operatedBy", deliveryNote.getId(), operatedBy);

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
    for (Edge purchOrderLine : purchOrderLines) {
      purchOrder = vertexMap.get(purchOrderLine.getSourceId());
      total = BigDecimal.ZERO;

      if (purchOrderTotals.containsKey(purchOrder)) {
        total = purchOrderTotals.get(purchOrder);
      }
      purchAmount = BigDecimal.valueOf(
        purchOrderLine.getPropertyValue("quantity").getInt());
      purchAmount = purchAmount.multiply(purchOrderLine.getPropertyValue(
        "purchPrice").getBigDecimal());
      total = total.add(purchAmount);
      purchOrderTotals.put(purchOrder, total);
    }

    List<Vertex> purchInvoices = Lists.newArrayList();

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
    String label = "PurchInvoice";
    Properties properties = new Properties();

    long purchOrderDate = purchOrder.getPropertyValue("date").getLong();
    long date = config.delayDelayConfiguration(purchOrderDate,
      getEdgeTargetQuality("placedAt", purchOrder.getId(),
        Constants.VENDOR_MAP), "PurchOrder", "invoiceDelay");

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("date", date);
    String bid = createBusinessIdentifier(currentId++, Constants.PURCHINVOICE_ACRONYM);
    properties.set("num", bid);
    properties.set("expense", total);
    properties.set("text", "*** TODO @ Brokerage ***");

    Vertex purchInvoice = newVertex(label, properties);

    newEdge("createdFor", purchInvoice.getId(), purchOrder.getId());

    return purchInvoice;
  }

  /**
   * Creates a new sales invoice.
   *
   * @param salesOrderLines sales order line, corresponding to the new invoice
   * @return vertex representation of a sales invoice
   */
  private Vertex newSalesInvoice(List<Edge> salesOrderLines) {
    String label = "SalesInvoice";
    Vertex salesOrder = vertexMap.get(salesOrderLines.get(0).getSourceId());
    Properties properties = new Properties();

    long salesOrderDate = salesOrder.getPropertyValue("date").getLong();
    long date = config.delayDelayConfiguration(salesOrderDate,
      getEdgeTargetQuality("processedBy", salesOrder.getId(),
        Constants.EMPLOYEE_MAP), "SalesOrder", "invoiceDelay");

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("date", date);
    String bid = createBusinessIdentifier(currentId++, Constants.SALESINVOICE_ACRONYM);
    properties.set("num", bid);
    properties.set("revenue", BigDecimal.ZERO);
    properties.set("text", "*** TODO @ Brokerage ***");

    Vertex salesInvoice = newVertex(label, properties);

    BigDecimal revenue;
    BigDecimal salesAmount;
    for (Edge salesOrderLine : salesOrderLines) {
      salesAmount = BigDecimal.valueOf(salesOrderLine.getPropertyValue(
        "quantity").getInt())
        .multiply(salesOrderLine.getPropertyValue("salesPrice").getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      revenue = salesInvoice.getPropertyValue("revenue").getBigDecimal();
      revenue = revenue.add(salesAmount);
      salesInvoice.setProperty("revenue", revenue);
    }

    newEdge("createdFor", salesInvoice.getId(), salesOrder.getId());

    return salesInvoice;
  }
}
