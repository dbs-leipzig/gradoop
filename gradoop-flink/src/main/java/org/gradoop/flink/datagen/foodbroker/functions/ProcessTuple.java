package org.gradoop.flink.datagen.foodbroker.functions;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.tuples.FoodBrokerMaps;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by Stephan on 13.09.16.
 */
public abstract class ProcessTuple<S, T> extends
  RichMapPartitionFunction<S, T> {
  /**
   * Foodbroker configuration
   */
  protected FoodBrokerConfig config;
  /**
   * iterator over all customers
   */
  protected Iterator<Map.Entry<GradoopId, Float>> customerIterator;
  /**
   * iterator over all vendors
   */
  protected Iterator<Map.Entry<GradoopId, Float>> vendorIterator;
  /**
   * iterator over all logistics
   */
  protected Iterator<Map.Entry<GradoopId, Float>> logisticIterator;
  /**
   * iterator over all employees
   */
  protected Iterator<Map.Entry<GradoopId, Float>> employeeIterator;
  /**
   * iterator over all product prices
   */
  protected Iterator<Map.Entry<GradoopId, Float>> productQualityIterator;
  /**
   * iterator over all product
   */
  protected Iterator<Map.Entry<GradoopId, BigDecimal>> productPriceIterator;
  /**
   * graph ids, one seperate id for each case
   */
  protected GradoopIdSet graphIds;
  /**
   * EPGM graph head factory
   */
  protected GraphHeadFactory graphHeadFactory;
  /**
   * EPGM vertex factory
   */
  protected VertexFactory vertexFactory;
  /**
   * EPGM edge factory
   */
  protected EdgeFactory edgeFactory;
  /**
   * set which contains all edges which are created
   */
  protected Set<Edge> edges;
  /**
   * map to quickly receive the target id of an edge
   */
  protected Map<Tuple2<String, GradoopId>, Set<GradoopId>> edgeMap;
  /**
   * map to get the vertex object of a given gradoop id
   */
  protected Map<GradoopId, Vertex> vertexMap;
  /**
   * map to get the customer quality of a given gradoop id
   */
  protected Map<GradoopId, Float> customerMap;
  /**
   * map to get the vendor quality of a given gradoop id
   */
  protected Map<GradoopId, Float> vendorMap;
  /**
   * map to get the logistic quality of a given gradoop id
   */
  protected Map<GradoopId, Float> logisticMap;
  /**
   * map to get the employee quality of a given gradoop id
   */
  protected Map<GradoopId, Float> emplyoeeMap;
  /**
   * map to get the prodouct quality of a given gradoop id
   */
  protected Map<GradoopId, Float> productQualityMap;
  /**
   * map to get the prodouct price of a given gradoop id
   */
  protected Map<GradoopId, BigDecimal> productPriceMap;

  protected Map<GradoopId, Float> userMap;

  protected long currentId = 1;
  protected long globalSeed;

  public ProcessTuple(EdgeFactory edgeFactory, VertexFactory vertexFactory,
    FoodBrokerConfig config, GraphHeadFactory graphHeadFactory) {
    this.edgeFactory = edgeFactory;
    vertexMap = Maps.newHashMap();
    edges = Sets.newHashSet();
    this.vertexFactory = vertexFactory;
    edgeMap = Maps.newHashMap();
    this.config = config;
    this.graphHeadFactory = graphHeadFactory;
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


  protected String createBusinessIdentifier(long seed, String acronym) {
    String seedString = String.valueOf(seed);
    String idString = String.valueOf(globalSeed);
    int count = 6 - idString.length();
    for(int i = 1; i <= (count); i++) {
      idString = "0" + idString;
    }
    count = 6 - seedString.length();
    for(int i = 1; i <= (count); i++) {
      seedString = "0" + seedString;
    }
    return acronym + idString + "-" + seedString;
  }

  /**
   * Creates a new edge from the given fields.
   *
   * @param label the edge label
   * @param source the source id
   * @param target the target id
   */
  protected void newEdge(String label, GradoopId source, GradoopId target) {
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
  protected Edge newEdge(String label, GradoopId source, GradoopId target,
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
   */
  protected Vertex newVertex(String label, PropertyList properties) {
    Vertex vertex = vertexFactory.createVertex(label, properties, graphIds);
    vertexMap.put(vertex.getId(), vertex);
    return vertex;
  }

  /**
   * Searches the master data tupel which is the edge target of the given
   * edge parameter.
   *
   * @param edgeLabel label of the edge
   * @param source source id
   * @return target master data tupel
   */
  protected GradoopId getEdgeTargetId(String edgeLabel, GradoopId source) {
    //there is always only one master data in this kind of edges
    return edgeMap.get(
      new Tuple2<>(edgeLabel, source)).iterator().next();
  }

  protected Float getEdgeTargetQuality(String edgeLabel, GradoopId source,
    String masterDataMap) {
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
    case Constants.USER_MAP:
      return userMap.get(target);
    default:
      return null;
    }
  }

  protected GradoopId getNextCustomer() {
    if (!customerIterator.hasNext()) {
      customerIterator = customerMap.entrySet().iterator();
    }
    return customerIterator.next().getKey();
  }

  protected GradoopId getNextVendor() {
    if (!vendorIterator.hasNext()) {
      vendorIterator = vendorMap.entrySet().iterator();
    }
    return vendorIterator.next().getKey();
  }

  protected GradoopId getNextLogistic() {
    if (!logisticIterator.hasNext()) {
      logisticIterator = logisticMap.entrySet().iterator();
    }
    return logisticIterator.next().getKey();
  }

  protected GradoopId getNextEmployee() {
    if (!employeeIterator.hasNext()) {
      employeeIterator = emplyoeeMap.entrySet().iterator();
    }
    return employeeIterator.next().getKey();
  }

  protected GradoopId getNextProduct() {
    if (!productQualityIterator.hasNext()) {
      productQualityIterator = productQualityMap.entrySet().iterator();
      productPriceIterator = productPriceMap.entrySet().iterator();
    }
    productPriceIterator.next();
    return productQualityIterator.next().getKey();
  }

  protected Set<Vertex> getVertices() {
    Set<Vertex> vertices = Sets.newHashSet();
    for(Map.Entry<GradoopId, Vertex> entry : vertexMap.entrySet()) {
      vertices.add(entry.getValue());
    }
    return vertices;
  }
}
