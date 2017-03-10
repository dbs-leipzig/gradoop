package org.gradoop.flink.io.impl.nested.fusion;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.nested.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.algorithms.nesting.GraphNestingWithModel;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;
import org.gradoop.flink.model.impl.nested.datastructures.NormalizedGraph;
import org.gradoop.flink.model.impl.nested.utils.RepresentationUtils;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by vasistas on 10/03/17.
 */
public class NestedFusionTest extends GradoopFlinkTestBase {

  /**
   * Defining the hashing functions required to break down the join function
   */
  protected FlinkAsciiGraphLoader getBibNetworkLoader() throws IOException {
    InputStream inputStream = getClass()
      .getResourceAsStream("/data/gdl/jointest.gdl");
    return getLoaderFromStream(inputStream);
  }

  @Test
  public void nestWithNothing() throws Exception {
    FlinkAsciiGraphLoader loader = getBibNetworkLoader();

    GraphCollection empty = loader.getGraphCollectionByVariables();
    DataLake driedLake = new DataLake(empty);
    IdGraphDatabase emptyIds = driedLake.getIdDatabase();

    GraphCollection data = loader.getGraphCollectionByVariables("research","citation");
    DataLake dataLake = new DataLake(data);
    IdGraphDatabase idbDataLake = dataLake.getIdDatabase();

    GraphNestingWithModel op = new GraphNestingWithModel();
    IdGraphDatabase result = dataLake.run(op).with(idbDataLake,emptyIds);

    // Checking if the graph returned is the same as the input one
    collectAndAssertTrue(RepresentationUtils.dataSetEquality(result.getGraphHeadToVertex(),
      idbDataLake.getGraphHeadToVertex()));
    System.out.print(result.getGraphHeadToEdge().count()+" "+idbDataLake.getGraphHeadToEdge().count());
    collectAndAssertTrue(RepresentationUtils.dataSetEquality(result.getGraphHeadToEdge(),
      idbDataLake.getGraphHeadToEdge()));
  }

  @Test
  public void nestWithSingleGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getBibNetworkLoader();

    // Loading the input
    GraphCollection data = loader.getGraphCollectionByVariables("research");
    DataLake dataLake = new DataLake(data);
    IdGraphDatabase idbDataLake = dataLake.getIdDatabase();

    /////
    //System.out.println("ToNestVertices: " + dataLake.asNormalizedGraph().getVertices().collect()+"\n\n");

    // Relations or nesting
    GraphCollection gch = loader.getGraphCollectionByVariables("g0","g1","g2","g3","g4");
    DataLake hgDataLake = new DataLake(gch);
    IdGraphDatabase hypervertices = hgDataLake.getIdDatabase();

    /////
    //System.out.println("Relations: " + hgDataLake.asNormalizedGraph().getVertices().collect()+"\n\n");

    //System.out.println(hypervertices.getGraphHeads().collect()+"\n\n");
    //System.out.println(RepresentationUtils.utilCanonicalRepresentation(
    //  hgDataLake.extractGraphFromGradoopId(hypervertices.getGraphHeads()).asNormalizedGraph()));

    // Loading the operator and evaluating the result through indices
    GraphNestingWithModel op = new GraphNestingWithModel();
    IdGraphDatabase result = dataLake.run(op).with(idbDataLake,hypervertices);

    // Loading the expected result from the dataset
    LogicalGraph expected = loader.getLogicalGraphByVariable("resulting");
    NormalizedGraph expectedGraph = new DataLake(expected).asNormalizedGraph();
    NormalizedGraph normalizedResult = result.asNormalizedGraph(dataLake);

    System.out.println(RepresentationUtils.utilCanonicalRepresentation(expected));
    System.out.println(" ~~~~ ");
    System.out.println(RepresentationUtils.utilCanonicalRepresentation(normalizedResult));

    // Check
    collectAndAssertTrue(normalizedResult.equalsByData(expectedGraph));
  }

  @Test
  public void nestWithGraphCollection() throws Exception {
    FlinkAsciiGraphLoader loader = getBibNetworkLoader();

    // Loading the input
    GraphCollection data = loader.getGraphCollectionByVariables("research","citation");
    DataLake dataLake = new DataLake(data);
    IdGraphDatabase idbDataLake = dataLake.getIdDatabase();

    // Relations or nesting
    GraphCollection gch = loader.getGraphCollectionByVariables("g0","g1","g2","g3","g4");
    DataLake hgDataLake = new DataLake(gch);
    IdGraphDatabase hypervertices = hgDataLake.getIdDatabase();

    System.out.println(hypervertices.getGraphHeads());

    // Loading the operator and evaluating the result through indices
    GraphNestingWithModel op = new GraphNestingWithModel();
    IdGraphDatabase result = dataLake.run(op).with(idbDataLake,hypervertices);

    // Loading the expected result from the dataset
    LogicalGraph expected = loader.getLogicalGraphByVariable("result");
    NormalizedGraph expectedGraph = new DataLake(expected).asNormalizedGraph();
    NormalizedGraph normalizedResult = result.asNormalizedGraph(dataLake);

    System.out.println(RepresentationUtils.utilCanonicalRepresentation(expected));
    System.out.println(" ~~~~ ");
    System.out.println(RepresentationUtils.utilCanonicalRepresentation(normalizedResult));

    // Check
    collectAndAssertTrue(normalizedResult.equalsByData(expectedGraph));
  }

}
