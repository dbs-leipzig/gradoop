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
import org.apache.flink.hadoop.shaded.org.apache.http.impl.cookie
  .DateParseException;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.cookie.DateUtils;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.*;
import org.gradoop.model.impl.datagen.foodbroker.tuples.AbstractMasterDataTuple;
import org.gradoop.model.impl.datagen.foodbroker.tuples.MasterDataTuple;
import org.gradoop.model.impl.datagen.foodbroker.tuples.ProductTuple;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class FoodBrokerage<G extends EPGMGraphHead,V extends EPGMVertex, E
  extends EPGMEdge> extends RichMapPartitionFunction<Long,
  Tuple2<Set<V>, Set<E>>>
  implements Serializable {

  private FoodBrokerConfig config;

  // nur die benötigten daten übergeben id,quality eine klasse extends tuple2

  private List<MasterDataTuple> customers;
  private List<MasterDataTuple> vendors;
  private List<MasterDataTuple> logistics;
  private List<MasterDataTuple> employees;
  private List<ProductTuple> products;
  //caseseeds als input

  private G graphHead;
  private GradoopIdSet graphIds;
  private EPGMGraphHeadFactory<G> graphHeadFactory;
  private EPGMVertexFactory<V> vertexFactory;
  private EPGMEdgeFactory<E> edgeFactory;

  private Set<V> vertices;
  private Set<E> edges;

  private Map<Tuple2<String, GradoopId>, GradoopId> edgeMap;
  private Map<GradoopId, V> vertexMap;
  private Map<GradoopId, AbstractMasterDataTuple> masterDataMap;

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
    masterDataMap = Maps.newHashMap();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    customers = getRuntimeContext().getBroadcastVariable(Customer
      .CLASS_NAME);
    vendors = getRuntimeContext().getBroadcastVariable(Vendor.CLASS_NAME);
    logistics = getRuntimeContext().getBroadcastVariable(Logistics.CLASS_NAME);
    employees = getRuntimeContext().getBroadcastVariable(Employee.CLASS_NAME);
    products = getRuntimeContext().getBroadcastVariable(Product.CLASS_NAME);

    initMasterDataMap();
  }

  @Override
  public void mapPartition(Iterable<Long> iterable,
    Collector<Tuple2<Set<V>, Set<E>>> collector) throws Exception {

    // SalesQuotation
    V salesQuotation;

    // SalesQuotationLines
    List<V> salesQuotationLines;
    // SalesOrder
    V salesOrder;

    // SalesOrderLines
    List<V> salesOrderLines;

    // PurchOrder
    List<V> purchOrders;

    // PurchOrderLine
    List<V> purchOrderLines;

    // DeliveryNotes
    List<V> deliveryNotes;

    // PurchInvoices
    List<V> purchInvoices;

    // SalesInvoices;
    V salesInvoice;

    long startDate = config.getStartDate();

    for (Long seed: iterable) {
      this.graphHead = graphHeadFactory.createGraphHead();
      this.graphIds = new GradoopIdSet();
      graphIds.add(this.graphHead.getId());

      // SalesQuotation
      salesQuotation = this.newSalesQuotation(startDate);

      // SalesQuotationLines

      salesQuotationLines = this.newSalesQuotationLines(salesQuotation);

      if (this.confirmed(salesQuotation)) {
        // SalesOrder
        salesOrder = this.newSalesOrder(salesQuotation);

        // SalesOrderLines
        salesOrderLines = this.newSalesOrderLines(salesOrder, salesQuotationLines);

        // newPurchOrders
        purchOrders = this.newPurchOrders(salesOrder, salesOrderLines);

        // PurchOrderLines
        purchOrderLines = this.newPurchOrderLines(purchOrders, salesOrderLines);

        // DeliveryNotes
        deliveryNotes = this.newDeliveryNotes(purchOrders);

        // PurchInvoices
        purchInvoices = newPurchInvoices(purchOrderLines);

        // SalesInvoices
        salesInvoice = newSalesInvoice(salesOrderLines);
      }
      collector.collect(new Tuple2<>(vertices, edges));
    }
  }

  private boolean confirmed(V salesQuotation) {
    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add((MasterDataTuple)getMasterDataEdgeTarget
      ("sentBy", salesQuotation.getId()));
    influencingMasterData.add((MasterDataTuple)getMasterDataEdgeTarget
      ("sentTo", salesQuotation.getId()));

    return config.happensTransitionConfiguration(influencingMasterData,
      "SalesQuotation", "confirmationProbability");
  }

  // SalesQuotation
  private V newSalesQuotation(long startDate){
    String label = "SalesQuotation";
    PropertyList properties = new PropertyList();

    properties.set("date",startDate);
    properties.set("kind","TransData");

    V salesQuotation = this.vertexFactory.createVertex(label, properties,
      this.graphIds);

    MasterDataTuple rndEmployee = getRandomTuple(employees);
    MasterDataTuple rndCustomer = getRandomTuple(customers);

    newEdge("sentBy", salesQuotation.getId(), rndEmployee.getId());
    newEdge("sentTo", salesQuotation.getId(), rndCustomer.getId());

    newVertex(salesQuotation);
    return salesQuotation;
  }

  // SalesQuotationLine
  private List<V> newSalesQuotationLines(V salesQuotation) {
    List<V> salesQuotationLines = new ArrayList<>();
    V salesQuotationLine;
    ProductTuple product;

    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add((MasterDataTuple)getMasterDataEdgeTarget
      ("sentBy", salesQuotation.getId()));
    influencingMasterData.add((MasterDataTuple)getMasterDataEdgeTarget
      ("sentTo", salesQuotation.getId()));

    int numberOfQuotationLines = config.getIntRangeConfigurationValue
      (influencingMasterData, "SalesQuotation", "lines");

    for (int i = 0; i < numberOfQuotationLines; i++) {
      product = getRandomTuple(this.products);
      salesQuotationLine = this.newSalesQuotationLine(salesQuotation,
        product);
      salesQuotationLines.add(salesQuotationLine);
    }
    return salesQuotationLines;
  }

  private V newSalesQuotationLine(V salesQuotation, ProductTuple product) {
    String label = "SalesQuotationLine";

    List<AbstractMasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add(getMasterDataEdgeTarget
      ("sentBy", salesQuotation.getId()));
    influencingMasterData.add(getMasterDataEdgeTarget
      ("sentTo", salesQuotation.getId()));

    influencingMasterData.add(product);

    BigDecimal salesMargin = config.getDecimalVariationConfigurationValue
      (influencingMasterData, "SalesQuotation", "salesMargin");

    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");
    properties.set("purchPrice", product.getPrice());
    properties.set("salesPrice",
      salesMargin
        .add(BigDecimal.ONE)
        .multiply(product.getPrice())
        .setScale(2,BigDecimal.ROUND_HALF_UP)
    );

    int quantity = config.getIntRangeConfigurationValue(
      new ArrayList<MasterDataTuple>(), "SalesQuotation", "lineQuantity");

    properties.set("quantity", quantity);

    V salesQuotationLine = this.vertexFactory.createVertex(label, properties, this.graphIds);

    newEdge("contains", salesQuotationLine.getId(), product.getId());
    newEdge("partOf", salesQuotationLine.getId(), salesQuotation.getId());

    newVertex(salesQuotationLine);
    return salesQuotationLine;
  }


  // SalesOrder
  private V newSalesOrder(V salesQuotation) {
    String label = "SalesOrder";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");

    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add((MasterDataTuple)getMasterDataEdgeTarget
      ("sentBy", salesQuotation.getId()));
    influencingMasterData.add((MasterDataTuple)getMasterDataEdgeTarget
      ("sentTo", salesQuotation.getId()));

    Long salesQuotationDate = null;
      salesQuotationDate = salesQuotation
        .getPropertyValue("date")
        .getLong();
    long date = config.delayDelayConfiguration(salesQuotationDate,
      influencingMasterData, "SalesQuotation", "confirmationDelay");
    properties.set("date", date);

    MasterDataTuple rndEmployee = getRandomTuple(employees);
    influencingMasterData.clear();
    influencingMasterData.add((MasterDataTuple)getMasterDataEdgeTarget
      ("sentTo", salesQuotation.getId()));
    influencingMasterData.add(rndEmployee);

    properties.set("deliveryDate", config.delayDelayConfiguration(date,
      influencingMasterData, "SalesOrder", "deliveryAgreementDelay"));

    V salesOrder = this.vertexFactory.createVertex(label, properties, this
      .graphIds);

    newEdge("receivedFrom", salesOrder.getId(), getMasterDataEdgeTarget("sentTo", salesQuotation
      .getId()).getId());
    newEdge("processedBy", salesOrder.getId(), rndEmployee.getId());
    newEdge("basedOn", salesOrder.getId(), salesQuotation.getId());

    newVertex(salesOrder);
    return salesOrder;
  }


  // SalesOrderLines
  private List<V> newSalesOrderLines(V salesOrder, List<V> salesQuotationLines) {
    List<V> salesOrderLines = new ArrayList<>();
    V salesOrderLine;

    for(V singleSalesQuotationLine : salesQuotationLines) {
      salesOrderLine = this.newSalesOrderLine(salesOrder,
        singleSalesQuotationLine);
      salesOrderLines.add(salesOrderLine);
    }

    return  salesOrderLines;
  }

  // SalesOrderLine
  private V newSalesOrderLine(V salesOrder, V salesQuotationLine) {
    String label = "SalesOrderLine";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");
    properties.set("salesPrice", salesQuotationLine.getPropertyValue
      ("salesPrice").getBigDecimal());
    properties.set("quantity", salesQuotationLine.getPropertyValue
      ("quantity").getInt());

    V salesOrderLine = this.vertexFactory.createVertex(label, properties, this.graphIds);


    newEdge("contains", salesOrderLine.getId(), getMasterDataEdgeTarget
      ("contains", salesQuotationLine.getId()).getId());
    newEdge("partOf", salesOrderLine.getId(), salesOrder.getId());

    newVertex(salesOrderLine);
    return salesOrderLine;
  }


  // PurchOrder
  private List<V> newPurchOrders(V salesOrder, List<V> salesOrderLines) {
    List<V> purchOrders = new ArrayList<>();
    V purchOrder;

    int numberOfVendors = config.getIntRangeConfigurationValue(new ArrayList
      <MasterDataTuple>(), "PurchOrder", "numberOfVendors");
    for (int i = 0; i < (numberOfVendors > salesOrderLines.size() ?
      salesOrderLines.size() : numberOfVendors); i++) {
      purchOrder = newPurchOrder(salesOrder, getRandomTuple(employees));

      purchOrders.add(purchOrder);
    }

    return purchOrders;
  }

  private V newPurchOrder(V salesOrder, MasterDataTuple processedBy) {
    V purchOrder;

    String label = "PurchOrder";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");

    long salesOrderDate = salesOrder.getPropertyValue("date")
        .getLong();
    long date = config.delayDelayConfiguration(salesOrderDate,
      (MasterDataTuple) getMasterDataEdgeTarget("processedBy", salesOrder.getId()),
      "PurchOrder", "purchaseDelay");

    properties.set("date", date);

    purchOrder = this.vertexFactory.createVertex(label, properties, this
      .graphIds);

    newEdge("serves", purchOrder.getId(), salesOrder.getId());
    newEdge("placedAt", purchOrder.getId(), getRandomTuple(vendors).getId());
    newEdge("processedBy", purchOrder.getId(), processedBy.getId());

    newVertex(purchOrder);
    return purchOrder;
  }


  // PurchOrderLine
  private List<V> newPurchOrderLines(List<V> purchOrders, List<V> salesOrderLines) {
    List<V> purchOrderLines = new ArrayList<>();
    V purchOrderLine;
    V purchOrder;

    int linesPerPurchOrder = salesOrderLines.size() / purchOrders.size();

    for (V singleSalesOrderLine : salesOrderLines){
      int purchOrderIndex = salesOrderLines.indexOf(singleSalesOrderLine) /
        linesPerPurchOrder;

      if (purchOrderIndex > (purchOrders.size()-1)){
        purchOrderIndex = purchOrders.size()-1;
      }

      purchOrder = purchOrders.get(purchOrderIndex);
      purchOrderLine = newPurchOrderLine(purchOrder,singleSalesOrderLine);

      purchOrderLines.add(purchOrderLine);
    }

    return purchOrderLines;
  }

  private V newPurchOrderLine(V purchOrder, V salesOrderLine) {
    V purchOrderLine;

    String label = "PurchOrderLine";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");
    properties.set("salesOrderLine", salesOrderLine.getId().toString());
    properties.set("quantity", salesOrderLine.getPropertyValue("quantity")
      .getInt());

    ProductTuple contains = (ProductTuple) getMasterDataEdgeTarget
      ("contains", salesOrderLine.getId());

    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add((MasterDataTuple)getMasterDataEdgeTarget
      ("processedBy", purchOrder.getId()));
    influencingMasterData.add((MasterDataTuple)getMasterDataEdgeTarget
      ("placedAt", purchOrder.getId()));

    BigDecimal purchPrice = contains.getPrice();
    purchPrice = BigDecimal.ONE
      .add(BigDecimal.ONE)
      .multiply(purchPrice)
      .setScale(2,BigDecimal.ROUND_HALF_UP);

    properties.set("purchPrice", purchPrice);

    purchOrderLine = this.vertexFactory.createVertex(label, properties, this.graphIds);

    salesOrderLine.setProperty("purchOrderLine", purchOrderLine.getId().toString());

    newEdge("contains", purchOrderLine.getId(), contains.getId());
    newEdge("partOf", purchOrderLine.getId(), purchOrder.getId());

    newVertex(purchOrderLine);
    return purchOrderLine;
  }


  // DeliveryNotes
  private List<V> newDeliveryNotes(List<V> purchOrders) {
    List<V> deliveryNotes = new ArrayList<>();
    V deliveryNote;
    for(V singlePurchOrder : purchOrders) {
      deliveryNote = newDeliveryNote(singlePurchOrder);

      deliveryNotes.add(deliveryNote);
    }

    return deliveryNotes;
  }

  private V newDeliveryNote(V purchOrder) {
    V deliveryNote;

    String label = "DeliveryNote";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");

    properties.set("trackingCode", "***TODO***");

    deliveryNote = this.vertexFactory.createVertex(label, properties, this.graphIds);

    long purchOrderDate = purchOrder.getPropertyValue("date")
        .getLong();
    MasterDataTuple operatedBy = getRandomTuple(logistics);
    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add(operatedBy);
    influencingMasterData.add((MasterDataTuple)getMasterDataEdgeTarget
      ("placedAt", purchOrder.getId()));

    long date = config.delayDelayConfiguration(purchOrderDate,
      influencingMasterData, "PurchOrder", "deliveryDelay");
    deliveryNote.setProperty("date", date);

    newEdge("contains", deliveryNote.getId(), purchOrder.getId());
    newEdge("operatedBy", deliveryNote.getId(), operatedBy.getId());

    newVertex(deliveryNote);
    return deliveryNote;
  }

  // PurchInvoices
  private List<V> newPurchInvoices(List<V> purchOrderLines) {
    V purchOrder;
    Map<V,BigDecimal> purchOrderTotals = new HashMap<>();

    BigDecimal total;
    BigDecimal purchAmount;
    for(V purchOrderLine : purchOrderLines){
      purchOrder = getVertexEdgeTarget("partOf", purchOrderLine.getId());

      total = BigDecimal.ZERO;

      if (purchOrderTotals.containsKey(purchOrder)){
        total = purchOrderTotals.get(purchOrder);
      }
      purchAmount = BigDecimal.valueOf(purchOrderLine.getPropertyValue("quantity")
        .getInt());
      purchAmount = purchAmount.multiply(purchOrderLine.getPropertyValue
        ("purchPrice").getBigDecimal());
      total = total.add(purchAmount);
      purchOrderTotals.put(purchOrder,total);
    }

    List<V> purchInvoices = new ArrayList<>();

    for(Map.Entry<V,BigDecimal> purchOrderTotal : purchOrderTotals.entrySet()){
      purchInvoices.add(newPurchInvoice(
        purchOrderTotal.getKey(),
        purchOrderTotal.getValue()
      ));
    }

    return purchInvoices;
  }

  private V newPurchInvoice(V purchOrder, BigDecimal total) {
    V purchInvoice;

    String label = "PurchInvoice";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");

    properties.set("expense", total);
    properties.set("text", "*** TODO @ FoodBrokerage ***");

    long purchOrderDate = purchOrder.getPropertyValue("date")
        .getLong();
    long date = config.delayDelayConfiguration(purchOrderDate,
      (MasterDataTuple)getMasterDataEdgeTarget("placedAt", purchOrder.getId()),
      "PurchOrder", "invoiceDelay");
    properties.set("date",  date);

    purchInvoice = this.vertexFactory.createVertex(label, properties, this.graphIds);

    newEdge("createdFor", purchInvoice.getId(), purchOrder.getId());

    newVertex(purchInvoice);
    return purchInvoice;
  }

  // SalesInvoices
  private V newSalesInvoice(List<V> salesOrderLines) {
    V salesInvoice;
    V salesOrder = getVertexEdgeTarget("partOf", salesOrderLines.get(0).getId
      ());

    String label = "SalesInvoice";

    PropertyList properties = new PropertyList();
    properties.set("kind","TransData");
    properties.set("revenue", BigDecimal.ZERO);
    properties.set("text", "*** TODO @ FoodBrokerage ***");

    long salesOrderDate = salesOrder.getPropertyValue("date")
        .getLong();
    long date = config.delayDelayConfiguration(salesOrderDate,
      (MasterDataTuple)getMasterDataEdgeTarget("processedBy", salesOrder.getId()),
      "SalesOrder", "invoiceDelay");
    properties.set("date",  date);

    salesInvoice = this.vertexFactory.createVertex(label, properties, this
      .graphIds);

    BigDecimal revenue;
    BigDecimal salesAmount;
    for (V salesOrderLine : salesOrderLines){
      salesAmount = BigDecimal.valueOf(salesOrderLine.getPropertyValue
        ("quantity").getInt())
        .multiply(salesOrderLine.getPropertyValue("salesPrice").getBigDecimal())
        .setScale(2,BigDecimal.ROUND_HALF_UP);
      revenue = salesInvoice.getPropertyValue("revenue").getBigDecimal();
      revenue = revenue.add(salesAmount);
      salesInvoice.setProperty("revenue", revenue);
    }

    newEdge("createdFor", salesInvoice.getId(), salesOrder.getId());

    newVertex(salesInvoice);
    return salesInvoice;
  }


  private void newEdge(String label, GradoopId source, GradoopId target) {
    edges.add(edgeFactory.createEdge(label, source, target, graphIds));
    edgeMap.put(new Tuple2<String, GradoopId>(label, source), target);
  }

  private void newVertex(V vertex) {
    vertices.add(vertex);
    vertexMap.put(vertex.getId(), vertex);
  }

  private AbstractMasterDataTuple getMasterDataEdgeTarget(String edgeLabel,
    GradoopId source) {
    return masterDataMap.get(edgeMap.get(new Tuple2<String, GradoopId>
      (edgeLabel, source)));
  }

  private V getVertexEdgeTarget(String edgeLabel,
    GradoopId source) {
    return vertexMap.get(edgeMap.get(new Tuple2<String, GradoopId>
      (edgeLabel, source)));
  }

  private<T extends AbstractMasterDataTuple> T getRandomTuple (List<T>
    list){
    //TODO rnd verbessern
    return list.get((int)Math.round(Math.random() * (list.size()-1)));
  }

  private void initMasterDataMap() {
    for (MasterDataTuple customer : customers) {
      masterDataMap.put(customer.getId(), customer);
    }
    for (MasterDataTuple vendor : vendors) {
      masterDataMap.put(vendor.getId(), vendor);
    }
    for (MasterDataTuple logistic : logistics) {
      masterDataMap.put(logistic.getId(), logistic);
    }
    for (MasterDataTuple employee : employees) {
      masterDataMap.put(employee.getId(), employee);
    }
    for (ProductTuple product : products) {
      masterDataMap.put(product.getId(), product);
    }
  }
}
