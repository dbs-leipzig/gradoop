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

package org.gradoop.model.impl.datagen.foodbroker.foodbrokerage;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.foodbroker.config.Constants;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.tuples.AbstractMasterDataTuple;
import org.gradoop.model.impl.datagen.foodbroker.tuples.MasterDataTuple;
import org.gradoop.model.impl.datagen.foodbroker.tuples.ProductTuple;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Map partition function with spreads the whole foodbrokerage process
 * equally to each worker.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class FoodBrokerage<G extends EPGMGraphHead, V extends EPGMVertex, E
  extends EPGMEdge>
  extends RichMapPartitionFunction<Long, GraphTransaction<G, V, E>>
  implements Serializable {

  /**
   * Foodbroker configuration
   */
  private FoodBrokerConfig config;

  // these tuples contains only the important data: id, quality
  /**
   * iterator over all customers
   */
  private Iterator<Map.Entry<GradoopId, MasterDataTuple>> customerIterator;
  /**
   * iterator over all vendors
   */
  private Iterator<Map.Entry<GradoopId, MasterDataTuple>> vendorIterator;
  /**
   * iterator over all logistics
   */
  private Iterator<Map.Entry<GradoopId, MasterDataTuple>> logisticIterator;
  /**
   * iterator over all employees
   */
  private Iterator<Map.Entry<GradoopId, MasterDataTuple>> employeeIterator;
  /**
   * iterator over all products
   */
  private Iterator<Map.Entry<GradoopId, ProductTuple>> productIterator;
  //caseseeds als input

  /**
   * graph ids, one seperate id for each case
   */
  private GradoopIdSet graphIds;
  /**
   * EPGM graph head factory
   */
  private EPGMGraphHeadFactory<G> graphHeadFactory;
  /**
   * EPGM vertex factory
   */
  private EPGMVertexFactory<V> vertexFactory;
  /**
   * EPGM edge factory
   */
  private EPGMEdgeFactory<E> edgeFactory;

  /**
   * set containing all vertices which are created
   */
  private Set<V> vertices;
  /**
   * set which contains all edges which are created
   */
  private Set<E> edges;

  /**
   * map to quickly receive the target id of an edge
   */
  private Map<Tuple2<String, GradoopId>, GradoopId> edgeMap;
  /**
   * map to get the vertex object of a given gradoop id
   */
  private Map<GradoopId, V> vertexMap;
  /**
   * map to get the customer data tuple of a given gradoop id
   */
  private Map<GradoopId, MasterDataTuple> customerDataMap;
  /**
   * map to get the vendor data tuple of a given gradoop id
   */
  private Map<GradoopId, MasterDataTuple> vendorDataMap;
  /**
   * map to get the logistic data tuple of a given gradoop id
   */
  private Map<GradoopId, MasterDataTuple> logisticDataMap;
  /**
   * map to get the employee data tuple of a given gradoop id
   */
  private Map<GradoopId, MasterDataTuple> emplyoeeDataMap;
  /**
   * map to get the prodouct data tuple of a given gradoop id
   */
  private Map<GradoopId, ProductTuple> productDataMap;

  /**
   * Valued consturctor
   *
   * @param graphHeadFactory EPGM graph head facroty
   * @param vertexFactory EPGM vertex factory
   * @param edgeFactory EPGM edge factory
   * @param config Foodbroker configuration
   */
  public FoodBrokerage(EPGMGraphHeadFactory<G> graphHeadFactory,
    EPGMVertexFactory<V> vertexFactory, EPGMEdgeFactory<E> edgeFactory,
    FoodBrokerConfig config) {

    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
    this.config = config;

    vertices = Sets.newHashSet();
    edges = Sets.newHashSet();

    edgeMap = Maps.newHashMap();
    vertexMap = Maps.newHashMap();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    customerDataMap = getRuntimeContext().<Map<GradoopId, MasterDataTuple>>
      getBroadcastVariable(Constants.CUSTOMERDATA_MAP).get(0);

    vendorDataMap = getRuntimeContext().<Map<GradoopId, MasterDataTuple>>
      getBroadcastVariable(Constants.VENDORDATA_MAP).get(0);

    logisticDataMap = getRuntimeContext().<Map<GradoopId, MasterDataTuple>>
      getBroadcastVariable(Constants.LOGISTICDATA_MAP).get(0);

    emplyoeeDataMap = getRuntimeContext().<Map<GradoopId, MasterDataTuple>>
      getBroadcastVariable(Constants.EMPLOYEEDATA_MAP).get(0);

    productDataMap = getRuntimeContext().<Map<GradoopId, ProductTuple>>
      getBroadcastVariable(Constants.PRODUCTDATA_MAP).get(0);

    customerIterator = customerDataMap.entrySet().iterator();
    vendorIterator = vendorDataMap.entrySet().iterator();
    logisticIterator = logisticDataMap.entrySet().iterator();
    employeeIterator = emplyoeeDataMap.entrySet().iterator();
    productIterator = productDataMap.entrySet().iterator();
  }

  @Override
  public void mapPartition(Iterable<Long> iterable,
    Collector<GraphTransaction<G, V, E>> collector) throws Exception {

    // SalesQuotation
    V salesQuotation;

    // SalesQuotationLines
    List<E> salesQuotationLines;
    // SalesOrder
    V salesOrder;

    // SalesOrderLines
    List<E> salesOrderLines;

    // PurchOrder
    List<V> purchOrders;

    // PurchOrderLine
    List<E> purchOrderLines;

    // DeliveryNotes
    List<V> deliveryNotes;

    // PurchInvoices
    List<V> purchInvoices;

    // SalesInvoices;
    V salesInvoice;

    long startDate = config.getStartDate();

    G graphHead;
    GraphTransaction<G, V, E> graphTransaction;

    for (Long seed: iterable) {
      vertices = Sets.newHashSet();
      edges = Sets.newHashSet();
      graphHead = graphHeadFactory.createGraphHead();
      graphIds = new GradoopIdSet();
      graphIds.add(graphHead.getId());
      graphTransaction = new GraphTransaction<>();

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
      graphTransaction.setVertices(vertices);
      graphTransaction.setEdges(edges);
      collector.collect(graphTransaction);
    }
  }

  /**
   * Checks if a sales quotation is confirmed.
   *
   * @param salesQuotation the quotation to be checked
   * @return true, if quotation is confirmed
   */
  private boolean confirmed(V salesQuotation) {
    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add((MasterDataTuple) getMasterDataEdgeTarget(
      "sentBy", salesQuotation.getId(), Constants.EMPLOYEEDATA_MAP));
    influencingMasterData.add((MasterDataTuple) getMasterDataEdgeTarget(
      "sentTo", salesQuotation.getId(), Constants.CUSTOMERDATA_MAP));

    return config.happensTransitionConfiguration(influencingMasterData,
      "SalesQuotation", "confirmationProbability");
  }

  /**
   * Creates a new sales quotation.
   *
   * @param startDate of the quotation
   * @return vertex representation of a sales quotation
   */
  private V newSalesQuotation(long startDate) {
    String label = "SalesQuotation";
    PropertyList properties = new PropertyList();

    properties.set("date", startDate);
    properties.set("kind", "TransData");

    V salesQuotation = vertexFactory.createVertex(label, properties, graphIds);

    MasterDataTuple rndEmployee = getNextEmplyoee();
    MasterDataTuple rndCustomer = getNextCustomer();

    newEdge("sentBy", salesQuotation.getId(), rndEmployee.getId());
    newEdge("sentTo", salesQuotation.getId(), rndCustomer.getId());

    newVertex(salesQuotation);
    return salesQuotation;
  }

  /**
   * Creates new sales quotation lines.
   *
   * @param salesQuotation quotation, corresponding to the lines
   * @return list of vertices which represent a sales quotation line
   */
  private List<E> newSalesQuotationLines(V salesQuotation) {
    List<E> salesQuotationLines = Lists.newArrayList();
    E salesQuotationLine;
    ProductTuple product;

    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add((MasterDataTuple) getMasterDataEdgeTarget(
      "sentBy", salesQuotation.getId(), Constants.EMPLOYEEDATA_MAP));
    influencingMasterData.add((MasterDataTuple) getMasterDataEdgeTarget(
      "sentTo", salesQuotation.getId(), Constants.CUSTOMERDATA_MAP));

    int numberOfQuotationLines = config.getIntRangeConfigurationValue(
      influencingMasterData, "SalesQuotation", "lines");

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
  private E newSalesQuotationLine(V salesQuotation, ProductTuple product) {
    String label = "SalesQuotationLine";

    List<AbstractMasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add(getMasterDataEdgeTarget(
      "sentBy", salesQuotation.getId(), Constants.EMPLOYEEDATA_MAP));
    influencingMasterData.add(getMasterDataEdgeTarget(
      "sentTo", salesQuotation.getId(), Constants.CUSTOMERDATA_MAP));

    influencingMasterData.add(product);

    BigDecimal salesMargin = config.getDecimalVariationConfigurationValue(
      influencingMasterData, "SalesQuotation", "salesMargin");

    PropertyList properties = new PropertyList();

    properties.set("kind", "TransData");
    properties.set("purchPrice", product.getPrice());
    properties.set("salesPrice",
      salesMargin
        .add(BigDecimal.ONE)
        .multiply(product.getPrice())
        .setScale(2, BigDecimal.ROUND_HALF_UP)
    );

    int quantity = config.getIntRangeConfigurationValue(
      new ArrayList<MasterDataTuple>(), "SalesQuotation", "lineQuantity");

    properties.set("quantity", quantity);

    E salesQuotationLine = newEdge(label, salesQuotation.getId(),
      product.getId(), properties);

    return salesQuotationLine;
  }

  /**
   * Creates a sales order for a confirmed sales quotation.
   *
   * @param salesQuotation the confirmed quotation
   * @return vertex representation of a sales order
   */
  private V newSalesOrder(V salesQuotation) {
    String label = "SalesOrder";
    PropertyList properties = new PropertyList();

    properties.set("kind", "TransData");

    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add((MasterDataTuple) getMasterDataEdgeTarget(
      "sentBy", salesQuotation.getId(), Constants.EMPLOYEEDATA_MAP));
    influencingMasterData.add((MasterDataTuple) getMasterDataEdgeTarget(
      "sentTo", salesQuotation.getId(), Constants.CUSTOMERDATA_MAP));

    Long salesQuotationDate = null;
    salesQuotationDate = salesQuotation
      .getPropertyValue("date")
      .getLong();
    long date = config.delayDelayConfiguration(salesQuotationDate,
      influencingMasterData, "SalesQuotation", "confirmationDelay");
    properties.set("date", date);

    MasterDataTuple rndEmployee = getNextEmplyoee();
    influencingMasterData.clear();
    influencingMasterData.add((MasterDataTuple) getMasterDataEdgeTarget
      ("sentTo", salesQuotation.getId(), Constants.CUSTOMERDATA_MAP));
    influencingMasterData.add(rndEmployee);

    properties.set("deliveryDate", config.delayDelayConfiguration(date,
      influencingMasterData, "SalesOrder", "deliveryAgreementDelay"));

    V salesOrder = vertexFactory.createVertex(label, properties, graphIds);

    newEdge("receivedFrom", salesOrder.getId(), getMasterDataEdgeTarget(
      "sentTo", salesQuotation.getId(), Constants.CUSTOMERDATA_MAP).getId());
    newEdge("processedBy", salesOrder.getId(), rndEmployee.getId());
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
  private List<E> newSalesOrderLines(V salesOrder,
    List<E> salesQuotationLines) {
    List<E> salesOrderLines = Lists.newArrayList();
    E salesOrderLine;

    for (E singleSalesQuotationLine : salesQuotationLines) {
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
  private E newSalesOrderLine(V salesOrder, E salesQuotationLine) {
    String label = "SalesOrderLine";
    PropertyList properties = new PropertyList();

    properties.set("kind", "TransData");
    properties.set("salesPrice", salesQuotationLine.getPropertyValue(
      "salesPrice").getBigDecimal());
    properties.set("quantity", salesQuotationLine.getPropertyValue(
      "quantity").getInt());

    E salesOrderLine = newEdge(label, salesOrder.getId(),
      salesQuotationLine.getTargetId(), properties);
    return salesOrderLine;
  }


  /**
   * Creates new purch orders.
   *
   * @param salesOrder sales order, corresponding to the new purch order
   * @param salesOrderLines lines, corresponding to the sales order
   * @return list of vertices which represent a purch order
   */
  private List<V> newPurchOrders(V salesOrder, List<E> salesOrderLines) {
    List<V> purchOrders = Lists.newArrayList();
    V purchOrder;

    int numberOfVendors = config.getIntRangeConfigurationValue(new ArrayList
      <MasterDataTuple>(), "PurchOrder", "numberOfVendors");
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
  private V newPurchOrder(V salesOrder, MasterDataTuple processedBy) {
    V purchOrder;

    String label = "PurchOrder";
    PropertyList properties = new PropertyList();

    properties.set("kind", "TransData");

    long salesOrderDate = salesOrder.getPropertyValue("date")
        .getLong();
    long date = config.delayDelayConfiguration(salesOrderDate,
      (MasterDataTuple) getMasterDataEdgeTarget("processedBy",
        salesOrder.getId(), Constants.EMPLOYEEDATA_MAP), "PurchOrder", "purchaseDelay");

    properties.set("date", date);

    purchOrder = vertexFactory.createVertex(label, properties, graphIds);

    newEdge("serves", purchOrder.getId(), salesOrder.getId());
    newEdge("placedAt", purchOrder.getId(), getNextVendor().getId());
    newEdge("processedBy", purchOrder.getId(), processedBy.getId());

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
  private List<E> newPurchOrderLines(List<V> purchOrders,
    List<E> salesOrderLines) {
    List<E> purchOrderLines = Lists.newArrayList();
    E purchOrderLine;
    V purchOrder;

    int linesPerPurchOrder = salesOrderLines.size() / purchOrders.size();

    for (E singleSalesOrderLine : salesOrderLines) {
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
  private E newPurchOrderLine(V purchOrder, E salesOrderLine) {
    E purchOrderLine;

    String label = "PurchOrderLine";
    PropertyList properties = new PropertyList();

    properties.set("kind", "TransData");
    properties.set("salesOrderLine", salesOrderLine.getId().toString());
    properties.set("quantity", salesOrderLine.getPropertyValue("quantity")
      .getInt());

    ProductTuple contains = productDataMap.get(salesOrderLine.getTargetId());

    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add((MasterDataTuple) getMasterDataEdgeTarget(
      "processedBy", purchOrder.getId(), Constants.EMPLOYEEDATA_MAP));
    influencingMasterData.add((MasterDataTuple) getMasterDataEdgeTarget(
      "placedAt", purchOrder.getId(), Constants.VENDORDATA_MAP));

    BigDecimal purchPrice = contains.getPrice();
    purchPrice = BigDecimal.ONE
      .add(BigDecimal.ONE)
      .multiply(purchPrice)
      .setScale(2, BigDecimal.ROUND_HALF_UP);

    properties.set("purchPrice", purchPrice);

    purchOrderLine = newEdge(label, purchOrder.getId(), contains.getId(),
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
  private List<V> newDeliveryNotes(List<V> purchOrders) {
    List<V> deliveryNotes = Lists.newArrayList();
    V deliveryNote;
    for (V singlePurchOrder : purchOrders) {
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
  private V newDeliveryNote(V purchOrder) {
    V deliveryNote;

    String label = "DeliveryNote";
    PropertyList properties = new PropertyList();

    properties.set("kind", "TransData");

    properties.set("trackingCode", "***TODO***");

    deliveryNote = vertexFactory.createVertex(label, properties, graphIds);

    long purchOrderDate = purchOrder.getPropertyValue("date")
        .getLong();
    MasterDataTuple operatedBy = getNextLogistic();
    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add(operatedBy);
    influencingMasterData.add((MasterDataTuple) getMasterDataEdgeTarget(
      "placedAt", purchOrder.getId(), Constants.VENDORDATA_MAP));

    long date = config.delayDelayConfiguration(purchOrderDate,
      influencingMasterData, "PurchOrder", "deliveryDelay");
    deliveryNote.setProperty("date", date);

    newEdge("contains", deliveryNote.getId(), purchOrder.getId());
    newEdge("operatedBy", deliveryNote.getId(), operatedBy.getId());

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
  private List<V> newPurchInvoices(List<E> purchOrderLines) {
    V purchOrder;
    Map<V, BigDecimal> purchOrderTotals = Maps.newHashMap();

    BigDecimal total;
    BigDecimal purchAmount;
    for (E purchOrderLine : purchOrderLines) {
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

    List<V> purchInvoices = Lists.newArrayList();

    for (Map.Entry<V, BigDecimal> purchOrderTotal : purchOrderTotals.entrySet())
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
  private V newPurchInvoice(V purchOrder, BigDecimal total) {
    V purchInvoice;

    String label = "PurchInvoice";
    PropertyList properties = new PropertyList();

    properties.set("kind", "TransData");

    properties.set("expense", total);
    properties.set("text", "*** TODO @ FoodBrokerage ***");

    long purchOrderDate = purchOrder.getPropertyValue("date")
        .getLong();
    long date = config.delayDelayConfiguration(purchOrderDate,
      (MasterDataTuple) getMasterDataEdgeTarget("placedAt", purchOrder.getId(),
        Constants.VENDORDATA_MAP),
      "PurchOrder", "invoiceDelay");
    properties.set("date",  date);

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
  private V newSalesInvoice(List<E> salesOrderLines) {
    V salesInvoice;
    V salesOrder = vertexMap.get(salesOrderLines.get(0).getSourceId());

    String label = "SalesInvoice";

    PropertyList properties = new PropertyList();
    properties.set("kind",  "TransData");
    properties.set("revenue", BigDecimal.ZERO);
    properties.set("text", "*** TODO @ FoodBrokerage ***");

    long salesOrderDate = salesOrder.getPropertyValue("date")
        .getLong();
    long date = config.delayDelayConfiguration(salesOrderDate,
      (MasterDataTuple) getMasterDataEdgeTarget(
        "processedBy", salesOrder.getId(), Constants.EMPLOYEEDATA_MAP), "SalesOrder", "invoiceDelay");
    properties.set("date", date);

    salesInvoice = vertexFactory.createVertex(label, properties, graphIds);

    BigDecimal revenue;
    BigDecimal salesAmount;
    for (E salesOrderLine : salesOrderLines) {
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
    edgeMap.put(new Tuple2<String, GradoopId>(label, source), target);
  }

  /**
   * Creates a new edge from the given fields.
   *
   * @param label the edge label
   * @param source the source id
   * @param target the target id
   * @param properties the edge properties
   */
  private E newEdge(String label, GradoopId source, GradoopId target,
    PropertyList properties) {
    E edge = edgeFactory.createEdge(label, source, target, properties,
      graphIds);
    edges.add(edge);
    edgeMap.put(new Tuple2<String, GradoopId>(label, source), target);
    return edge;
  }

  /**
   * Stores a newly created vertex.
   *
   * @param vertex the vertex to store
   */
  private void newVertex(V vertex) {
    vertices.add(vertex);
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
  private AbstractMasterDataTuple getMasterDataEdgeTarget(String edgeLabel,
    GradoopId source, String masterDataMap) {
    GradoopId target = edgeMap.get(
      new Tuple2<String, GradoopId>(edgeLabel, source));
    switch (masterDataMap) {
      case Constants.CUSTOMERDATA_MAP :
        return customerDataMap.get(target);
      case Constants.VENDORDATA_MAP :
        return vendorDataMap.get(target);
      case Constants.LOGISTICDATA_MAP :
        return logisticDataMap.get(target);
      case Constants.EMPLOYEEDATA_MAP :
        return emplyoeeDataMap.get(target);
      case Constants.PRODUCTDATA_MAP :
        return productDataMap.get(target);
      default:
        return null;
    }
  }

  private MasterDataTuple getNextCustomer() {
    return customerIterator.next().getValue();
  }

  private MasterDataTuple getNextVendor() {
    return vendorIterator.next().getValue();
  }

  private MasterDataTuple getNextLogistic() {
    return logisticIterator.next().getValue();
  }

  private MasterDataTuple getNextEmplyoee() {
    return employeeIterator.next().getValue();
  }

  private ProductTuple getNextProduct() {
    return productIterator.next().getValue();
  }

}
