package org.gradoop.model.impl.datagen.foodbroker;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.api.datagen.GraphGenerator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.functions.Customer;
import org.gradoop.model.impl.datagen.foodbroker.functions.Employee;
import org.gradoop.model.impl.datagen.foodbroker.functions.ForeignKey;
import org.gradoop.model.impl.datagen.foodbroker.functions.MasterDataVertex;
import org.gradoop.model.impl.datagen.foodbroker.functions.PrimaryKey;
import org.gradoop.model.impl.datagen.foodbroker.functions.Product;
import org.gradoop.model.impl.datagen.foodbroker.functions.RandomForeignKey;
import org.gradoop.model.impl.datagen.foodbroker.functions.SalesQuotation;
import org.gradoop.model.impl.datagen.foodbroker.functions
  .SalesQuotationConfirmation;
import org.gradoop.model.impl.datagen.foodbroker.functions.SalesQuotationLine;
import org.gradoop.model.impl.datagen.foodbroker.functions
  .SalesQuotationLineContains;
import org.gradoop.model.impl.datagen.foodbroker.functions
  .SalesQuotationLineQuality;
import org.gradoop.model.impl.datagen.foodbroker.functions.SalesQuotationSentBy;
import org.gradoop.model.impl.datagen.foodbroker.functions.SalesQuotationSentTo;
import org.gradoop.model.impl.datagen.foodbroker.functions.SalesQuotationWon;
import org.gradoop.model.impl.datagen.foodbroker.functions.TransactionalVertex;
import org.gradoop.model.impl.datagen.foodbroker.generator.CaseGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.CustomerGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.EmployeeGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.LogisticsGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.ProductGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.VendorGenerator;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.TransactionalDataObject;
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

    DataSet<MasterDataObject> customers = new CustomerGenerator(
      env, foodBrokerConfig).generate();

    Integer customerCount =
      foodBrokerConfig.getMasterDataCount(Customer.CLASS_NAME);

    // Vendor

    DataSet<MasterDataObject> vendors = new VendorGenerator(
      env, foodBrokerConfig).generate();

    DataSet<MasterDataObject> logistics = new LogisticsGenerator(
      env, foodBrokerConfig).generate();

    // Employee

    DataSet<MasterDataObject> employees = new EmployeeGenerator(
      env, foodBrokerConfig).generate();

    Integer employeeCount =
      foodBrokerConfig.getMasterDataCount(Employee.CLASS_NAME);

    // Product

    DataSet<MasterDataObject> products = new ProductGenerator(
      env, foodBrokerConfig).generate();

    Integer productCount =
      foodBrokerConfig.getMasterDataCount(Product.CLASS_NAME);

    DataSet<Long> cases = new CaseGenerator(
      env, foodBrokerConfig).generate();

    EPGMVertexFactory<V> vertexFactory = gradoopFlinkConfig.getVertexFactory();

    DataSet<V> masterDataVertices = customers
      .map(new MasterDataVertex<>(vertexFactory))
      .union(vendors.map(new MasterDataVertex<>(vertexFactory)))
      .union(logistics.map(new MasterDataVertex<>(vertexFactory)))
      .union(employees.map(new MasterDataVertex<>(vertexFactory)))
      .union(products.map(new MasterDataVertex<>(vertexFactory)))      ;


    DataSet<TransactionalDataObject> salesQuotations = cases
      .map(new SalesQuotation())
      .map(new RandomForeignKey(employeeCount))
      .join(employees)
      .where(new ForeignKey()).equalTo(new PrimaryKey())
      .with(new SalesQuotationSentBy())
      .map(new RandomForeignKey(customerCount))
      .join(customers)
      .where(new ForeignKey()).equalTo(new PrimaryKey())
      .with(new SalesQuotationSentTo());

    // SalesQuotationLine

    Integer minLines = foodBrokerConfig.getMinSalesQuotationLines();
    Integer maxLines = foodBrokerConfig.getMaxQuotationLines();

    DataSet<TransactionalDataObject> salesQuotationLines = salesQuotations
      .flatMap(new SalesQuotationLine(minLines, maxLines))
      .map(new RandomForeignKey(productCount))
      .join(products)
      .where(new ForeignKey()).equalTo(new PrimaryKey())
      .with(new SalesQuotationLineContains());

    // Confirmation

    Float probability = foodBrokerConfig
      .getSalesQuotationConfirmationProbability();
    Float probabilityInfluence = foodBrokerConfig
      .getSalesQuotationConfirmationProbabilityInfluence();

    salesQuotations = salesQuotationLines
      .groupBy(5)
      .reduceGroup(new SalesQuotationLineQuality())
      .join(salesQuotations)
      .where(0).equalTo(0)
      .with(new SalesQuotationConfirmation(probability, probabilityInfluence));

    DataSet<V> transactionalVertices = salesQuotations
      .map(new TransactionalVertex<>(vertexFactory));

    salesQuotations = salesQuotations.filter(new SalesQuotationWon());

    try {
      transactionalVertices.print();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return LogicalGraph.createEmptyGraph(gradoopFlinkConfig);
  }
}
