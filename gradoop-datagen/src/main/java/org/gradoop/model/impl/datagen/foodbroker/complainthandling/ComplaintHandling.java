package org.gradoop.model.impl.datagen.foodbroker.complainthandling;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.Customer;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.Employee;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.Logistics;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.Product;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.Vendor;
import org.gradoop.model.impl.datagen.foodbroker.tuples.AbstractMasterDataTuple;
import org.gradoop.model.impl.datagen.foodbroker.tuples.MasterDataTuple;
import org.gradoop.model.impl.datagen.foodbroker.tuples.ProductTuple;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ComplaintHandling<G extends EPGMGraphHead, V extends EPGMVertex,
  E extends EPGMEdge> extends
  RichMapPartitionFunction<V, Tuple2<Set<V>, Set<E>>> implements Serializable {

  private GradoopIdSet graphIds;
  private final EPGMVertexFactory<V> vertexFactory;
  private final EPGMEdgeFactory<E> edgeFactory;
  private final FoodBrokerConfig config;

  private List<MasterDataTuple> customers;
  private List<MasterDataTuple> vendors;
  private List<MasterDataTuple> logistics;
  private List<MasterDataTuple> employees;
  private List<ProductTuple> products;

  private List<V> purchOrderLines;
  private List<Tuple2<V, V>> purchesToLines;
  private List<V> salesOrderLines;
  private List<Tuple2<V, V>> salesToLines;
  private List<Tuple3<V, V, V>> salesToDeliveries;
  private List<E> transactionalEdges;

  private Set<V> vertices;
  private Set<E> edges;

  private Map<Tuple2<String, GradoopId>, GradoopId> edgeMap;
  private Map<GradoopId, V> vertexMap;
  private Map<GradoopId, AbstractMasterDataTuple> masterDataMap;

  public ComplaintHandling(EPGMVertexFactory<V> vertexFactory,
    EPGMEdgeFactory<E> edgeFactory, FoodBrokerConfig config) {
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
    this.config = config;

    vertices = Sets.newHashSet();
    edges = Sets.newHashSet();

    edgeMap = Maps.newHashMap();
    vertexMap = Maps.newHashMap();
    masterDataMap = Maps.newHashMap();
  }

  private void lateDelivery(V sale) {
    final List<V> deliveryNotes = getDeliveryNotesBySale(sale);
    final List<V> lateSalesOrderLines = new ArrayList<>();
    for (V deliveryNote : deliveryNotes) {
      if (deliveryNote.getPropertyValue("date").getLong() >
        sale.getPropertyValue("deliveryDate").getLong()) {
        lateSalesOrderLines.addAll(getSalesOrderLinesBySale(sale));
      }
    }

    if (!lateSalesOrderLines.isEmpty()) {
      List<V> latePurchOrderLines = new ArrayList<>();
      for (V salesOrderLine : lateSalesOrderLines) {
        latePurchOrderLines
          .add(getPurchOrderLineBySalesOrderLine(salesOrderLine));
      }
      Calendar calendar = Calendar.getInstance();
      calendar.setTimeInMillis(sale.getPropertyValue("deliveryDate").getLong());
      calendar.add(Calendar.DATE, 1);
      long createdDate = calendar.getTimeInMillis();

      V ticket = newTicket(sale, "late delivery", createdDate);
      grantSalesRefund(sale, lateSalesOrderLines, ticket);
      claimPurchRefund(latePurchOrderLines, ticket);
    }
  }

  private void badQuality(V sale) {
    final List<V> deliveryNotes = getDeliveryNotesBySale(sale);

    for (V deliveryNote : deliveryNotes) {
      List<AbstractMasterDataTuple> influencingMasterData =
        Lists.newArrayList();

      V purchOrder = getPurchOrderByDeliveryNote(deliveryNote);
      List<V> purchOrderLines = getPurchOrderLinesByPurchOrder(purchOrder);

      for (V purchOrderLine : purchOrderLines) {
        influencingMasterData
          .add(getMasterDataEdgeTarget("contains", purchOrderLine.getId()));
      }

      int containedProducts = influencingMasterData.size();

      // increase relative influence of vendor and logistics
      for (int i = 1; i <= containedProducts / 2; i++) {
        influencingMasterData
          .add(getMasterDataEdgeTarget("operatedBy", deliveryNote.getId()));
        influencingMasterData
          .add(getMasterDataEdgeTarget("placedAt", purchOrder.getId()));
      }

      if (config.happensTransitionConfiguration(influencingMasterData, "Ticket",
        "badQualityProbability")) {
        List<V> badSalesOrderLines = new ArrayList<>();
        for (V purchOrderLine : purchOrderLines) {
          badSalesOrderLines
            .add(getSalesOrderLineByPurchOrderLine(purchOrderLine));
        }

        V ticket = newTicket(sale, "bad quality",
          deliveryNote.getPropertyValue("date").getLong());
        grantSalesRefund(sale, badSalesOrderLines, ticket);
        claimPurchRefund(purchOrderLines, ticket);
      }
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    customers = getRuntimeContext().getBroadcastVariable(Customer.CLASS_NAME);
    vendors = getRuntimeContext().getBroadcastVariable(Vendor.CLASS_NAME);
    logistics = getRuntimeContext().getBroadcastVariable(Logistics.CLASS_NAME);
    employees = getRuntimeContext().getBroadcastVariable(Employee.CLASS_NAME);
    products = getRuntimeContext().getBroadcastVariable(Product.CLASS_NAME);

    purchOrderLines =
      getRuntimeContext().getBroadcastVariable("purchOrderLines");
    purchesToLines = getRuntimeContext().getBroadcastVariable("purchesToLines");
    salesOrderLines =
      getRuntimeContext().getBroadcastVariable("salesOrderLines");
    salesToLines = getRuntimeContext().getBroadcastVariable("salesToLines");
    salesToDeliveries =
      getRuntimeContext().getBroadcastVariable("salesToDeliveries");
    transactionalEdges =
      getRuntimeContext().getBroadcastVariable("transactionalEdges");

    initMasterDataMap();
  }

  @Override
  public void mapPartition(Iterable<V> values,
    Collector<Tuple2<Set<V>, Set<E>>> out) throws Exception {
    for (V sale : values) {
      vertices = Sets.newHashSet();
      edges = Sets.newHashSet();
      graphIds = sale.getGraphIds();

      lateDelivery(sale);
      badQuality(sale);

      out.collect(new Tuple2<>(vertices, edges));
    }
  }

  /* ----- Association fetches ----- */

  private V getPurchOrderLineBySalesOrderLine(V salesOrderLine) {
    for (V purchOrderLine : purchOrderLines) {
      if (purchOrderLine.getPropertyValue("salesOrderLine").getString()
        .equals(salesOrderLine.getId().toString())) {
        return purchOrderLine;
      }
    }
    return null;
  }

  private List<V> getDeliveryNotesBySale(V sale) {
    List<V> result = new ArrayList<>();
    for (Tuple3<V, V, V> t : salesToDeliveries) {
      if (t.f0.getId().equals(sale.getId())) {
        result.add(t.f2);
      }
    }
    return result;
  }

  private List<V> getSalesOrderLinesBySale(V saleVertex) {
    List<V> lines = new ArrayList<>();
    for (Tuple2<V, V> t : salesToLines) {
      if (t.f0.getId().equals(saleVertex.getId())) {
        lines.add(t.f1);
      }
    }
    return lines;
  }

  private V getPurchOrderByDeliveryNote(V deliveryNote) {
    for (Tuple3<V, V, V> t : salesToDeliveries) {
      if (t.f2.getId().equals(deliveryNote.getId())) {
        return t.f1;
      }
    }
    return null;
  }

  private List<V> getPurchOrderLinesByPurchOrder(V purchOrder) {
    List<V> lines = new ArrayList<>();
    for (Tuple2<V, V> t : purchesToLines) {
      if (t.f0.getId().equals(purchOrder.getId())) {
        lines.add(t.f1);
      }
    }
    return lines;
  }

  private V getSalesOrderLineByPurchOrderLine(V purchOrderLine) {
    for (V salesOrderLine : salesOrderLines) {
      if (salesOrderLine.getPropertyValue("purchOrderLine").getString()
        .equals(purchOrderLine.getId().toString())) {
        return salesOrderLine;
      }
    }
    return null;
  }

  private V getPurchOrderByPurchOrderLine(V purchOrderLine) {
    for (Tuple2<V, V> t : purchesToLines) {
      if (t.f1.getId().equals(purchOrderLine.getId())) {
        return t.f0;
      }
    }
    return null;
  }

  private <T extends AbstractMasterDataTuple> T getRandomTuple(List<T> list) {
    //TODO rnd verbessern
    return list.get((int) Math.round(Math.random() * (list.size() - 1)));
  }

  /* ----- Creation of new elements ----- */

  private V newTicket(V sale, String type, long createdDate) {
    String label = "Ticket";
    PropertyList properties = new PropertyList();

    properties.set("createdAt", createdDate);
    properties.set("problem", type);
    properties.set("kind", "TransData");

    MasterDataTuple customerTuple =
      (MasterDataTuple) getMasterDataEdgeTarget("receivedFrom", sale.getId());

    V ticket =
      vertexFactory.createVertex(label, properties, sale.getGraphIds());
    newEdge("createdBy", ticket.getId(),
      getRandomTuple(employees).getId()); // TODO: User?!
    newEdge("allocatedTo", ticket.getId(),
      getRandomTuple(employees).getId()); // TODO: User?!
    newEdge("openedBy", ticket.getId(),
      customerTuple.getId()); // TODO: Clients?!

    newVertex(ticket);

    return ticket;
  }

  /* ----- Refunds ----- */

  private void grantSalesRefund(V sale, List<V> salesOrderLines, V ticket) {
    List<AbstractMasterDataTuple> influencingMasterData = new ArrayList<>();
    influencingMasterData
      .add(getMasterDataEdgeTarget("allocatedTo", ticket.getId()));
    influencingMasterData
      .add(getMasterDataEdgeTarget("receivedFrom", sale.getId()));

    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(influencingMasterData, "Ticket",
        "salesRefund");
    BigDecimal refundAmount = BigDecimal.ZERO;

    for (V salesOrderLine : salesOrderLines) {
      refundAmount = refundAmount
        .add(salesOrderLine.getPropertyValue("salesPrice").getBigDecimal());
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);

    if (refundAmount.floatValue() < 0) {
      V salesInvoice;
      String label = "SalesInvoice";

      PropertyList properties = new PropertyList();
      properties.set("kind", "TransData");
      properties.set("revenue", BigDecimal.ZERO);
      properties.set("text", "*** TODO @ ComplaintHandling ***");
      properties.set("date", ticket.getPropertyValue("createdAt").getLong());
      properties.set("revenue", refundAmount);

      salesInvoice = vertexFactory.createVertex(label, properties);

      newEdge("createdFor", salesInvoice.getId(), sale.getId());
      newVertex(salesInvoice);
    }
  }

  private void claimPurchRefund(List<V> purchOrderLines, V ticket) {
    V purchOrder = getPurchOrderByPurchOrderLine(purchOrderLines.get(0));

    List<AbstractMasterDataTuple> influencingMasterData = new ArrayList<>();
    influencingMasterData
      .add(getMasterDataEdgeTarget("allocatedTo", ticket.getId()));
    influencingMasterData
      .add(getMasterDataEdgeTarget("placedAt", purchOrder.getId()));

    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(influencingMasterData, "Ticket",
        "purchRefund");
    BigDecimal refundAmount = BigDecimal.ZERO;

    for (V purchOrderLine : purchOrderLines) {
      refundAmount = refundAmount
        .add(purchOrderLine.getPropertyValue("purchPrice").getBigDecimal());
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);

    if (refundAmount.floatValue() < 0) {
      V purchInvoice;

      String label = "PurchInvoice";
      PropertyList properties = new PropertyList();

      properties.set("kind", "TransData");

      properties.set("expense", refundAmount);
      properties.set("text", "*** TODO @ ComplaintHandling ***");
      properties.set("date", ticket.getPropertyValue("createdAt").getLong());

      purchInvoice = this.vertexFactory
        .createVertex(label, properties, ticket.getGraphIds()); // TODO

      newEdge("createdFor", purchInvoice.getId(), purchOrder.getId());
      newVertex(purchInvoice);
    }
  }

  /* ----- Creation of Edges and Vertices ----- */

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
    GradoopId id =
      edgeMap.get(new Tuple2<String, GradoopId>(edgeLabel, source));
    if (id == null) {
      for (E transactionalEdge : transactionalEdges) {
        if (edgeLabel.equals(transactionalEdge.getLabel()) &&
          source.equals(transactionalEdge.getSourceId())) {
          id = transactionalEdge.getTargetId();
          break;
        }
      }
    }
    return masterDataMap.get(id);
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