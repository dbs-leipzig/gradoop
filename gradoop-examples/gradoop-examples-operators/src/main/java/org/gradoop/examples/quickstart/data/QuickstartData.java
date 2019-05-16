package org.gradoop.examples.quickstart.data;

/**
 * Class to provide the graph data for the QuickstartExample
 */
public class QuickstartData {

  /**
   * Example Graph for QuickstartExample
   *
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Getting-started">
   * Gradoop Quickstart Example</a>
   * @return example graph
   */
  public static String getGraphGDLString() {

    return "" +
      "g1:graph[\n" +
      "      (p1:Person {name: \"Bob\", age: 24})-[:friendsWith]->\n" +
      "      (p2:Person{name: \"Alice\", age: 30})-[:friendsWith]->(p1)\n" +
      "      (p2)-[:friendsWith]->(p3:Person {name: \"Jacob\", age: 27})-[:friendsWith]->(p2)\n" +
      "      (p3)-[:friendsWith]->(p4:Person{name: \"Marc\", age: 40})-[:friendsWith]->(p3)\n" +
      "      (p4)-[:friendsWith]->(p5:Person{name: \"Sara\", age: 33})-[:friendsWith]->(p4)\n" +
      "      (c1:Company {name: \"Acme Corp\"})\n" +
      "      (c2:Company {name: \"Globex Inc.\"})\n" +
      "      (p2)-[:worksAt]->(c1)\n" +
      "      (p4)-[:worksAt]->(c1)\n" +
      "      (p5)-[:worksAt]->(c1)\n" +
      "      (p1)-[:worksAt]->(c2)\n" +
      "      (p3)-[:worksAt]->(c2)" +
      "      ]\n" +
      "      g2:graph[\n" +
      "      (p4)-[:friendsWith]->(p6:Person {name: \"Paul\", age: 37})-[:friendsWith]->(p4)\n" +
      "      (p6)-[:friendsWith]->(p7:Person {name: \"Mike\", age: 23})-[:friendsWith]->(p6)\n" +
      "      (p8:Person {name: \"Jil\", age: 32})-[:friendsWith]->(p7)-[:friendsWith]->(p8)\n" +
      "      (p6)-[:worksAt]->(c2)\n" +
      "      (p7)-[:worksAt]->(c2)\n" +
      "      (p8)-[:worksAt]->(c1)]";
  }
}
