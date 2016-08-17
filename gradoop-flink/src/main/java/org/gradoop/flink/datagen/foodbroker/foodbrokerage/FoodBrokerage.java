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

package org.gradoop.flink.datagen.foodbroker.foodbrokerage;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.tuples.FoodBrokerMaps;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Map partition function which spreads the whole foodbrokerage process
 * equally to each worker.
 */
public class FoodBrokerage
  extends RichMapPartitionFunction<Long, Tuple2<GraphTransaction, FoodBrokerMaps>>
  implements Serializable {

  /**
   * Foodbroker configuration
   */
  private FoodBrokerConfig config;

  // these tuples contains only the important data: id, quality
  /**
   * iterator over all customers
   */
  private Iterator<Map.Entry<GradoopId, Float>> customerIterator;
  /**
   * iterator over all vendors
   */
  private Iterator<Map.Entry<GradoopId, Float>> vendorIterator;
  /**
   * iterator over all logistics
   */
  private Iterator<Map.Entry<GradoopId, Float>> logisticIterator;
  /**
   * iterator over all employees
   */
  private Iterator<Map.Entry<GradoopId, Float>> employeeIterator;
  /**
   * iterator over all product prices
   */
  private Iterator<Map.Entry<GradoopId, Float>> productQualityIterator;
  /**
   * iterator over all product
   */
  private Iterator<Map.Entry<GradoopId, BigDecimal>> productPriceIterator;

  /**
   * graph ids, one seperate id for each case
   */
  private GradoopIdSet graphIds;
  /**
   * EPGM graph head factory
   */
  private GraphHeadFactory graphHeadFactory;
  /**
   * EPGM vertex factory
   */
  private VertexFactory vertexFactory;
  /**
   * EPGM edge factory
   */
  private EdgeFactory edgeFactory;

//  /**
//   * set containing all vertices which are created
//   */
//  private Set<Vertex> vertices;
  /**
   * set which contains all edges which are created
   */
  private Set<Edge> edges;

  /**
   * map to quickly receive the target id of an edge
   */
  private Map<Tuple2<String, GradoopId>, Set<GradoopId>> edgeMap;
  /**
   * map to get the vertex object of a given gradoop id
   */
  private Map<GradoopId, Vertex> vertexMap;
  /**
   * map to get the customer quality of a given gradoop id
   */
  private Map<GradoopId, Float> customerMap;
  /**
   * map to get the vendor quality of a given gradoop id
   */
  private Map<GradoopId, Float> vendorMap;
  /**
   * map to get the logistic quality of a given gradoop id
   */
  private Map<GradoopId, Float> logisticMap;
  /**
   * map to get the employee quality of a given gradoop id
   */
  private Map<GradoopId, Float> emplyoeeMap;
  /**
   * map to get the prodouct quality of a given gradoop id
   */
  private Map<GradoopId, Float> productQualityMap;
  /**
   * map to get the prodouct price of a given gradoop id
   */
  private Map<GradoopId, BigDecimal> productPriceMap;

  /**
   * Valued consturctor
   *
   * @param graphHeadFactory EPGM graph head facroty
   * @param vertexFactory EPGM vertex factory
   * @param edgeFactory EPGM edge factory
   * @param config Foodbroker configuration
   */
  public FoodBrokerage(GraphHeadFactory graphHeadFactory,
    VertexFactory vertexFactory, EdgeFactory edgeFactory,
    FoodBrokerConfig config) {

    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
    this.config = config;

//    vertices = Sets.newHashSet();
    edges = Sets.newHashSet();

    edgeMap = Maps.newHashMap();
    vertexMap = Maps.newHashMap();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    customerMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.CUSTOMER_MAP).get(0);

    vendorMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.VENDOR_MAP).get(0);

    logisticMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.LOGISTIC_MAP).get(0);

    emplyoeeMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.EMPLOYEE_MAP).get(0);

    productQualityMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.PRODUCT_QUALITY_MAP).get(0);

    productPriceMap = getRuntimeContext().<Map<GradoopId, BigDecimal>>
      getBroadcastVariable(Constants.PRODUCT_PRICE_MAP).get(0);

    customerIterator = customerMap.entrySet().iterator();
    vendorIterator = vendorMap.entrySet().iterator();
    logisticIterator = logisticMap.entrySet().iterator();
    employeeIterator = emplyoeeMap.entrySet().iterator();
    productQualityIterator = productQualityMap.entrySet().iterator();
    productPriceIterator = productPriceMap.entrySet().iterator();
  }

  @Override
  public void mapPartition(Iterable<Long> iterable,
    Collector<Tuple2<GraphTransaction, FoodBrokerMaps>> collector) throws
    Exception {

    // SalesQuotation
    Vertex salesQuotation;

    // SalesQuotationLines
    List<Edge> salesQuotationLines;
    // SalesOrder
    Vertex salesOrder;

    // SalesOrderLines
    List<Edge> salesOrderLines;

    // PurchOrder
    List<Vertex> purchOrders;

    // PurchOrderLine
    List<Edge> purchOrderLines;

    // DeliveryNotes
    List<Vertex> deliveryNotes;

    // PurchInvoices
    List<Vertex> purchInvoices;

    // SalesInvoices;
    Vertex salesInvoice;

    long startDate = config.getStartDate();

    GraphHead graphHead;
    GraphTransaction graphTransaction;

    for (Long seed: iterable) {
      vertexMap = Maps.newHashMap();
      edgeMap = Maps.newHashMap();
//      vertices = Sets.newHashSet();
      edges = Sets.newHashSet();
      graphHead = graphHeadFactory.createGraphHead();
      graphIds = new GradoopIdSet();
      graphIds.add(graphHead.getId());
      graphTransaction = new GraphTransaction();

      // SalesQuotation
      salesQuotation = newSalesQuotation(startDate);

      // SalesQuotationLines

      salesQuotationLines = newSalesQuotationLines(salesQuotation);

      if (confirmed(salesQuotation)) {
        // SalesOrder
        salesOrder = newSalesOrder(salesQuotation);

        // SalesOrderLines
        salesOrderLines = newSalesOrderLines(salesOrder, salesQuotationLines);

        // newPurchOrders
        purchOrders = newPurchOrders(salesOrder, salesOrderLines);

        // PurchOrderLines
        purchOrderLines = newPurchOrderLines(purchOrders, salesOrderLines);

        // DeliveryNotes
        deliveryNotes = newDeliveryNotes(purchOrders);

        // PurchInvoices
        purchInvoices = newPurchInvoices(purchOrderLines);

        // SalesInvoices
        salesInvoice = newSalesInvoice(salesOrderLines);
      }
      graphTransaction.setGraphHead(graphHead);
      graphTransaction.setVertices(getVertices());
      graphTransaction.setEdges(edges);
      collector.collect(new Tuple2<>(graphTransaction,
        new FoodBrokerMaps(vertexMap, edgeMap)));
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
    PropertyList properties = new PropertyList();

    properties.set("date", startDate);
    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);

    Vertex salesQuotation = vertexFactory.createVertex(label, properties, 
      graphIds);

    GradoopId rndEmployee = getNextEmplyoee();
    GradoopId rndCustomer = getNextCustomer();

    newEdge("sentBy", salesQuotation.getId(), rndEmployee);
    newEdge("sentTo", salesQuotation.getId(), rndCustomer);

    newVertex(salesQuotation);
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
      salesQuotationLine = newSalesQuotationLine(salesQuotation,
        product);
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

    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentBy", salesQuotation.getId(), Constants.EMPLOYEE_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentTo", salesQuotation.getId(), Constants.CUSTOMER_MAP));

    influencingMasterQuality.add(productQualityMap.get(product));

    BigDecimal salesMargin = config.getDecimalVariationConfigurationValue(
      influencingMasterQuality, "SalesQuotation", "salesMargin");

    PropertyList properties = new PropertyList();

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("purchPrice", productPriceMap.get(product));
    properties.set("salesPrice",
      salesMargin
        .add(BigDecimal.ONE)
        .multiply(productPriceMap.get(product))
        .setScale(2, BigDecimal.ROUND_HALF_UP)
    );

    int quantity = config.getIntRangeConfigurationValue(
      new ArrayList<Float>(), "SalesQuotation", "lineQuantity");

    properties.set("quantity", quantity);

    return newEdge(label, salesQuotation.getId(),
      product, properties);
  }

  /**
   * Creates a sales order for a confirmed sales quotation.
   *
   * @param salesQuotation the confirmed quotation
   * @return vertex representation of a sales order
   */
  private Vertex newSalesOrder(Vertex salesQuotation) {
    String label = "SalesOrder";
    PropertyList properties = new PropertyList();

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);

    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentBy", salesQuotation.getId(), Constants.EMPLOYEE_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentTo", salesQuotation.getId(), Constants.CUSTOMER_MAP));

    Long salesQuotationDate;
    salesQuotationDate = salesQuotation
      .getPropertyValue("date")
      .getLong();
    long date = config.delayDelayConfiguration(salesQuotationDate,
      influencingMasterQuality, "SalesQuotation", "confirmationDelay");
    properties.set("date", date);

    GradoopId rndEmployee = getNextEmplyoee();
    influencingMasterQuality.clear();
    influencingMasterQuality.add(getEdgeTargetQuality(
      "sentTo", salesQuotation.getId(), Constants.CUSTOMER_MAP));
    influencingMasterQuality.add(emplyoeeMap.get(rndEmployee));

    properties.set("deliveryDate", config.delayDelayConfiguration(date,
      influencingMasterQuality, "SalesOrder", "deliveryAgreementDelay"));

    Vertex salesOrder = vertexFactory.createVertex(label, properties, graphIds);

    newEdge("receivedFrom", salesOrder.getId(), getEdgeTargetId(
      "sentTo", salesQuotation.getId()));
    newEdge("processedBy", salesOrder.getId(), rndEmployee);
    newEdge("basedOn", salesOrder.getId(), salesQuotation.getId());

    newVertex(salesOrder);
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
    PropertyList properties = new PropertyList();

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

    int numberOfVendors = config.getIntRangeConfigurationValue(new ArrayList
      <Float>(), "PurchOrder", "numberOfVendors");
    for (int i = 0; i < (numberOfVendors > salesOrderLines.size() ?
      salesOrderLines.size() : numberOfVendors); i++) {
      purchOrder = newPurchOrder(salesOrder, getNextEmplyoee());

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
    Vertex purchOrder;

    String label = "PurchOrder";
    PropertyList properties = new PropertyList();

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);

    long salesOrderDate = salesOrder.getPropertyValue("date")
        .getLong();
    long date = config.delayDelayConfiguration(salesOrderDate,
      getEdgeTargetQuality("processedBy", salesOrder.getId(),
        Constants.EMPLOYEE_MAP), "PurchOrder", "purchaseDelay");

    properties.set("date", date);

    purchOrder = vertexFactory.createVertex(label, properties, graphIds);

    newEdge("serves", purchOrder.getId(), salesOrder.getId());
    newEdge("placedAt", purchOrder.getId(), getNextVendor());
    newEdge("processedBy", purchOrder.getId(), processedBy);

    newVertex(purchOrder);
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
    Edge purchOrderLine;

    String label = "PurchOrderLine";
    PropertyList properties = new PropertyList();

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("salesOrderLine", salesOrderLine.getId().toString());
    properties.set("quantity", salesOrderLine.getPropertyValue("quantity")
      .getInt());

    BigDecimal price = productPriceMap.get(salesOrderLine.getTargetId());

    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality(
      "processedBy", purchOrder.getId(), Constants.EMPLOYEE_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(
      "placedAt", purchOrder.getId(), Constants.VENDOR_MAP));

    BigDecimal purchPrice = price;
    purchPrice = BigDecimal.ONE
      .add(BigDecimal.ONE)
      .multiply(purchPrice)
      .setScale(2, BigDecimal.ROUND_HALF_UP);

    properties.set("purchPrice", purchPrice);

    purchOrderLine = newEdge(label, purchOrder.getId(), salesOrderLine.getTargetId(),
      properties);

    salesOrderLine.setProperty(
      "purchOrderLine", purchOrderLine.getId().toString());

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
    Vertex deliveryNote;

    String label = "DeliveryNote";
    PropertyList properties = new PropertyList();

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);

    properties.set("trackingCode", "***TODO***");

    deliveryNote = vertexFactory.createVertex(label, properties, graphIds);

    long purchOrderDate = purchOrder.getPropertyValue("date")
        .getLong();
    GradoopId operatedBy = getNextLogistic();
    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(logisticMap.get(operatedBy));
    influencingMasterQuality.add(getEdgeTargetQuality(
      "placedAt", purchOrder.getId(), Constants.VENDOR_MAP));

    long date = config.delayDelayConfiguration(purchOrderDate,
      influencingMasterQuality, "PurchOrder", "deliveryDelay");
    deliveryNote.setProperty("date", date);

    newEdge("contains", deliveryNote.getId(), purchOrder.getId());
    newEdge("operatedBy", deliveryNote.getId(), operatedBy);

    newVertex(deliveryNote);
    return deliveryNote;
  }

  /**
   * Creates new purch invoices.
   *
   * @param purchOrderLines purch order lines, corresponding to the new
   *                        invoices
   * @return list of vertices which represent a purch invoice
   */
  private List newPurchInvoices(List<Edge> purchOrderLines) {
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

    for (Map.Entry<Vertex, BigDecimal> purchOrderTotal : purchOrderTotals
      .entrySet())
    {
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
    Vertex purchInvoice;

    String label = "PurchInvoice";
    PropertyList properties = new PropertyList();

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);

    properties.set("expense", total);
    properties.set("text", "*** TODO @ FoodBrokerage ***");

    long purchOrderDate = purchOrder.getPropertyValue("date")
        .getLong();
    long date = config.delayDelayConfiguration(purchOrderDate,
      getEdgeTargetQuality("placedAt", purchOrder.getId(),
        Constants.VENDOR_MAP), "PurchOrder", "invoiceDelay");
    properties.set("date", date);

    purchInvoice = vertexFactory.createVertex(label, properties, graphIds);

    newEdge("createdFor", purchInvoice.getId(), purchOrder.getId());

    newVertex(purchInvoice);
    return purchInvoice;
  }

  /**
   * Creates a new sales invoice.
   *
   * @param salesOrderLines sales order line, corresponding to the new invoice
   * @return vertex representation of a sales invoice
   */
  private Vertex newSalesInvoice(List<Edge> salesOrderLines) {
    Vertex salesInvoice;
    Vertex salesOrder = vertexMap.get(salesOrderLines.get(0).getSourceId());

    String label = "SalesInvoice";

    PropertyList properties = new PropertyList();
    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("revenue", BigDecimal.ZERO);
    properties.set("text", "*** TODO @ FoodBrokerage ***");

    long salesOrderDate = salesOrder.getPropertyValue("date")
        .getLong();
    long date = config.delayDelayConfiguration(salesOrderDate,
      getEdgeTargetQuality("processedBy", salesOrder.getId(),
        Constants.EMPLOYEE_MAP), "SalesOrder", "invoiceDelay");
    properties.set("date", date);

    salesInvoice = vertexFactory.createVertex(label, properties, graphIds);

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

    newVertex(salesInvoice);
    return salesInvoice;
  }

  /**
   * Creates a new edge from the given fields.
   *
   * @param label the edge label
   * @param source the source id
   * @param target the target id
   */
  private void newEdge(String label, GradoopId source, GradoopId target) {
    edges.add(edgeFactory.createEdge(label, source, target, graphIds));
    Tuple2<String, GradoopId> key = new Tuple2<>(label, source);
    Set<GradoopId> targets = Sets.newHashSet();
    if (edgeMap.containsKey(key)) {
      targets = edgeMap.get(key);
    }
    targets.add(target);
    edgeMap.put(key, targets);
  }

  /**
   * Creates a new edge from the given fields.
   *
   * @param label the edge label
   * @param source the source id
   * @param target the target id
   * @param properties the edge properties
   */
  private Edge newEdge(String label, GradoopId source, GradoopId target,
    PropertyList properties) {
    Edge edge = edgeFactory.createEdge(label, source, target, properties,
      graphIds);
    edges.add(edge);
    Tuple2<String, GradoopId> key = new Tuple2<>(label, source);
    Set<GradoopId> targets = Sets.newHashSet();
    if (edgeMap.containsKey(key)) {
      targets = edgeMap.get(key);
    }
    targets.add(target);
    edgeMap.put(key, targets);
    return edge;
  }

  /**
   * Stores a newly created vertex.
   *
   * @param vertex the vertex to store
   */
  private void newVertex(Vertex vertex) {
//    vertices.add(vertex);
    vertexMap.put(vertex.getId(), vertex);
  }

  /**
   * Searches the master data tupel which is the edge target of the given
   * edge parameter.
   *
   * @param edgeLabel label of the edge
   * @param source source id
   * @return target master data tupel
   */
  private GradoopId getEdgeTargetId(String edgeLabel,
    GradoopId source) {
    //there is always only one master data in this kind of edges
    return edgeMap.get(
      new Tuple2<>(edgeLabel, source)).iterator().next();
  }

  private Float getEdgeTargetQuality(String edgeLabel,
    GradoopId source, String masterDataMap) {
    GradoopId target = getEdgeTargetId(edgeLabel, source);

    switch (masterDataMap) {
    case Constants.CUSTOMER_MAP:
      return customerMap.get(target);
    case Constants.VENDOR_MAP:
      return vendorMap.get(target);
    case Constants.LOGISTIC_MAP:
      return logisticMap.get(target);
    case Constants.EMPLOYEE_MAP:
      return emplyoeeMap.get(target);
    default:
      return null;
    }
  }


  private GradoopId getNextCustomer() {
    if (!customerIterator.hasNext()) {
      customerIterator = customerMap.entrySet().iterator();
    }
    return customerIterator.next().getKey();
  }

  private GradoopId getNextVendor() {
    if (!vendorIterator.hasNext()) {
      vendorIterator = vendorMap.entrySet().iterator();
    }
    return vendorIterator.next().getKey();
  }

  private GradoopId getNextLogistic() {
    if (!logisticIterator.hasNext()) {
      logisticIterator = logisticMap.entrySet().iterator();
    }
    return logisticIterator.next().getKey();
  }

  private GradoopId getNextEmplyoee() {
    if (!employeeIterator.hasNext()) {
      employeeIterator = emplyoeeMap.entrySet().iterator();
    }
    return employeeIterator.next().getKey();
  }

  private GradoopId getNextProduct() {
    if (!productQualityIterator.hasNext()) {
      productQualityIterator = productQualityMap.entrySet().iterator();
      productPriceIterator = productPriceMap.entrySet().iterator();
    }
    productPriceIterator.next();
    return productQualityIterator.next().getKey();
  }

  private Set<Vertex> getVertices() {
    Set<Vertex> vertices = Sets.newHashSet();
    for(Map.Entry<GradoopId, Vertex> entry : vertexMap.entrySet()) {
      vertices.add(entry.getValue());
    }
    return vertices;
  }
}
