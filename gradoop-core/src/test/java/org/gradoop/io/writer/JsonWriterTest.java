//package org.gradoop.io.writer;
//
//import org.gradoop.GradoopTest;
//import org.gradoop.model.VertexData;
//import org.json.JSONException;
//import org.junit.Test;
//import org.skyscreamer.jsonassert.JSONAssert;
//
///**
// * Tests for {@link org.gradoop.io.writer.JsonWriter}.
// */
//public class JsonWriterTest extends GradoopTest {
//
//  @Test
//  public void writerTest() throws JSONException {
//    VertexLineWriter writer = new JsonWriter();
//    int i = 0;
//    for (VertexData v : createExtendedGraphVertices()) {
//      JSONAssert.assertEquals(EXTENDED_GRAPH_JSON[i], writer.writeVertex(v), false);
//      i++;
//    }
//  }
//}
