package org.gradoop.model.impl.algorithms.btg;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class BusinessTransactionGraphsTest extends GradoopFlinkTestBase {

  @Test
  public void testExecute() throws Exception {

    String masterDataProperty = BusinessTransactionGraphs.SUPERCLASS_KEY
      + "= \"" + BusinessTransactionGraphs.MASTERDATA_VALUE + "\"";

    String transactionalDataProperty = BusinessTransactionGraphs.SUPERCLASS_KEY
      + "=\"" + BusinessTransactionGraphs.TRANSDATA_VALUE + "\"";

    String asciiGraphs = "" +
      "iig[" +
      // master data objects
      "(m1{#m#})-->(m2{#m#});" +
      // transactional data objects
      "(t11{#t#});(t12{#t#});(t13{#t#});" +
      "(t21{#t#});(t22{#t#});(t23{#t#});" +
      // transactional - transactional
      "(t12)-[t121]->(t11)<-[t131]-(t13);" +
      "(t22)-[t221]->(t21)<-[t231]-(t23);" +
      // transactional - master
      "(t11)-[m112]->(m2);" +
      "(t12)-[m121]->(m1)<-[m131]-(t13);" +
      "(t23)-[m232]->(m2);" +
      "(t22)-[m221]->(m1)<-[m211]-(t21);" +
      "];" +
      "btg1[" +
      "(t12)-[t121]->(t11)<-[t131]-(t13);" +
      "(t11)-[m112]->(m2);" +
      "(t12)-[m121]->(m1)<-[m131]-(t13);" +
      "" +
      "]" +
      "btg2[" +
      "(t22)-[t221]->(t21)<-[t231]-(t23);" +
      "(t11)-[m112]->(m2);" +
      "(t12)-[m121]->(m1)<-[m131]-(t13);" +
      "" +
      "]";

    asciiGraphs = asciiGraphs
      .replaceAll("#t#", transactionalDataProperty)
      .replaceAll("#m#", masterDataProperty);

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> iig =
      loader.getLogicalGraphByVariable("iig");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("btg1", "btg2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      iig.callForCollection(
        new BusinessTransactionGraphs<GraphHeadPojo,VertexPojo, EdgePojo>());

    collectAndAssertEquals(expectation.equalsByGraphElementIds(result));
  }
}