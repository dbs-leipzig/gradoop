package org.gradoop.flink.io.impl.nested;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.nested.datastructures.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;
import org.gradoop.flink.model.impl.nested.datastructures.NormalizedGraph;
import org.gradoop.flink.model.impl.nested.utils.RepresentationUtils;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * Checking if the conversion from normal LogicalGraph and GraphCollection to elements with
 * ids and so on is ok.
 */
public class NestedConversionTest extends GradoopFlinkTestBase {

  /**
   * Defining the hashing functions required to break down the join function
   */
  protected FlinkAsciiGraphLoader getBibNetworkLoader() throws IOException {
    InputStream inputStream = getClass()
      .getResourceAsStream("/data/gdl/jointest.gdl");
    return getLoaderFromStream(inputStream);
  }

  @Test
  public void testConversionSingleGraph() throws Exception {
    LogicalGraph research = getBibNetworkLoader().getLogicalGraphByVariable("research");
    DataLake lake = new DataLake(research);
    IdGraphDatabase igd = lake.getIdDatabase();
    NormalizedGraph toCompare = igd.asNormalizedGraph(lake.asNormalizedGraph());
    collectAndAssertTrue(toCompare.equalsByData(lake.asNormalizedGraph()));
  }

  @Test
  public void testConversionGraphCollectionWithOne() throws Exception {
    GraphCollection research = getBibNetworkLoader().getGraphCollectionByVariables("g0");
    DataLake lake = new DataLake(research);
    IdGraphDatabase igd = lake.getIdDatabase();
    NormalizedGraph toCompare = igd.asNormalizedGraph(lake.asNormalizedGraph());
    collectAndAssertTrue(toCompare.equalsByData(lake.asNormalizedGraph()));
  }

  @Test
  public void testConversionGraphCollectionWithManyElements() throws Exception {
    GraphCollection research = getBibNetworkLoader().getGraphCollectionByVariables("g0","g1",
      "g2","g3","g4");
    DataLake lake = new DataLake(research);
    IdGraphDatabase igd = lake.getIdDatabase();
    NormalizedGraph toCompare = igd.asNormalizedGraph(lake.asNormalizedGraph());
    collectAndAssertTrue(toCompare.equalsByData(lake.asNormalizedGraph()));
  }

  @Test
  public void testConversionGraphCollectionWithManyElementsExtractOne() throws Exception {
    LogicalGraph lg = getBibNetworkLoader().getLogicalGraphByVariable("g0");
    NormalizedGraph n0 = new NormalizedGraph(lg);

    //Loading the collection from the dataset
    GraphCollection research = getBibNetworkLoader().getGraphCollectionByVariables("g0","g1",
      "g2","g3","g4");

    //Creating a data-lake from the extended graph network
    DataLake lake = new DataLake(research);

    //Retrieving its id-only database
    IdGraphDatabase igd = lake.getIdDatabase();

    //From the datalake, I extract a graph where the head has an unique and distiguishible label.
    //We also pass a GradoopId or a collection of GradoopId
    DataLake dl = lake.extractGraphFromLabel("first");

    collectAndAssertTrue(dl.asNormalizedGraph().equalsByData(n0));
  }

  @Test
  public void testForLabelExtraction() throws Exception {
    //Loading the collection from the dataset
    GraphCollection research = getBibNetworkLoader().getGraphCollectionByVariables("g5","g6");

    //Creating a data-lake from the extended graph network
    DataLake lake = new DataLake(research);

    //From the datalake, I extract a graph where the head has an unique and distiguishible label.
    //We also pass a GradoopId or a collection of GradoopId
    DataLake dl = lake.extractGraphFromLabel("sixth");

    collectAndAssertTrue(dl.asNormalizedGraph().equalsByData(lake.asNormalizedGraph()));
  }

}
