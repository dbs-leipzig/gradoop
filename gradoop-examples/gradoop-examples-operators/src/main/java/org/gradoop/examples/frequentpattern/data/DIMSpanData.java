package org.gradoop.examples.frequentpattern.data;

/**
 * Provides example data for the DIMSpanExample
 */
public class DIMSpanData {

  /**
   * Provides an example graph collection used for DIMSpanExample
   *
   * @return example graph collection
   */
  public static String getGraphGDLString() {

    return "" +
      "g1[" +
      "   (v1:A)-->(v2:B)" +
      "   (v1)-->(v3:C)" +
      "]" +
      "g2[" +
      "   (v4:A)-->(v5:B)" +
      "   (v4)-->(v6:C)" +
      "   (v4)-->(v7:D)" +
      "   (v5)-->(v6)" +
      "]" +
      "g3[" +
      "   (v8:A)-->(v9:B)" +
      "   (v8)-->(v10:C)" +
      "   (v8)-->(v11:D)" +
      "   (v8)-->(v12:E)" +
      "   (v9)-->(v10)" +
      "]";

  }

}
