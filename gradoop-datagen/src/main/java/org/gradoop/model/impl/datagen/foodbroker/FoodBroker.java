package org.gradoop.model.impl.datagen.foodbroker;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.api.datagen.GraphGenerator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.functions.SalesQuotationWon;
import org.gradoop.model.impl.datagen.foodbroker.functions.SalesQuoationConfirmation;
import org.gradoop.model.impl.datagen.foodbroker.functions.SalesQuotationLinePartOf;
import org.gradoop.model.impl.datagen.foodbroker.functions.CreateSalesQuotation;
import org.gradoop.model.impl.datagen.foodbroker.functions
  .SalesQuotationLineContains;
import org.gradoop.model.impl.datagen.foodbroker.functions.SalesQuotationSentBy;
import org.gradoop.model.impl.datagen.foodbroker.functions.SalesQuotationSentTo;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotation;
import org.gradoop.model.impl.datagen.foodbroker.generator.CaseGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.CustomerGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.EmployeeGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.LogisticsGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.ProductGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.VendorGenerator;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotationLine;
import org.gradoop.util.GradoopFlinkConfig;


public class FoodBroker
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements GraphGenerator<G, V, E> {

  private final GradoopFlinkConfig<G, V, E> gradoopFlinkConfig;
  private final FoodBrokerConfig foodBrokerConfig;
  protected final ExecutionEnvironment env;

  public FoodBroker(ExecutionEnvironment env,
    GradoopFlinkConfig<G, V, E> gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig) {

    this.gradoopFlinkConfig = gradoopFlinkConfig;
    this.foodBrokerConfig = foodBrokerConfig;
    this.env = env;
  }

  @Override
  public LogicalGraph<G, V, E> generate(Integer scaleFactor) {

    foodBrokerConfig.setScaleFactor(scaleFactor);

    // Customer

    DataSet<MasterDataObject> customers = new CustomerGenerator<>(
      env, foodBrokerConfig, gradoopFlinkConfig.getVertexFactory()).generate();

    Integer customerCount =
      foodBrokerConfig.getMasterDataCount(CustomerGenerator.CLASS_NAME);

    // Vendor

    DataSet<MasterDataObject> vendors = new VendorGenerator<>(
      env, foodBrokerConfig, gradoopFlinkConfig.getVertexFactory()).generate();

    DataSet<MasterDataObject> logistics = new LogisticsGenerator<>(
      env, foodBrokerConfig, gradoopFlinkConfig.getVertexFactory()).generate();

    // Employee

    DataSet<MasterDataObject> employees = new EmployeeGenerator<>(
      env, foodBrokerConfig, gradoopFlinkConfig.getVertexFactory()).generate();

    Integer employeeCount =
      foodBrokerConfig.getMasterDataCount(EmployeeGenerator.CLASS_NAME);

    // Product

    DataSet<MasterDataObject> products = new ProductGenerator<>(
      env, foodBrokerConfig, gradoopFlinkConfig.getVertexFactory()).generate();

    Integer productCount =
      foodBrokerConfig.getMasterDataCount(ProductGenerator.CLASS_NAME);

    DataSet<Long> cases = new CaseGenerator<V, E>(
      env, foodBrokerConfig).generate();

    EPGMVertexFactory vertexFactory = gradoopFlinkConfig.getVertexFactory();
    EPGMEdgeFactory<E> edgeFactory = gradoopFlinkConfig.getEdgeFactory();

    DataSet<MasterDataObject> vertices = customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products);

    // SalesQuotation

    DataSet<SalesQuotation> salesQuotations = cases
      .map(new CreateSalesQuotation(employeeCount, customerCount))
      .join(employees)
      .where(1).equalTo(0)
      .with(new SalesQuotationSentBy())
      .join(customers)
      .where(3).equalTo(0)
      .with(new SalesQuotationSentTo());

    // SalesQuotationLine

    Integer minLines = foodBrokerConfig.getMinQuotationLines();
    Integer maxLines = foodBrokerConfig.getMaxQuotationLines();

    DataSet<SalesQuotationLine> salesQuotationLines = salesQuotations
      .flatMap(new SalesQuotationLinePartOf(productCount, minLines, maxLines));

    salesQuotationLines = salesQuotationLines.join(products)
      .where(1).equalTo(0)
      .with(new SalesQuotationLineContains());

    // Confirmation

    Float probability = foodBrokerConfig.getProbability();
    Float probabilityInfluence = foodBrokerConfig.getProbabilityInfluence();

    salesQuotations = salesQuotationLines
      .groupBy(0)
      .sum(2)
      .join(salesQuotations)
      .where(0).equalTo(5)
      .with(new SalesQuoationConfirmation(probability, probabilityInfluence));

    salesQuotations = salesQuotations.filter(new SalesQuotationWon());

    try {
      salesQuotations.print();
    } catch (Exception e) {
      e.printStackTrace();
    }


    return LogicalGraph.createEmptyGraph(gradoopFlinkConfig);
  }
}
