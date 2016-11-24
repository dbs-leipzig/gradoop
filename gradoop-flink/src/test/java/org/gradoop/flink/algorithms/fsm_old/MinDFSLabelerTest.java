package org.gradoop.flink.algorithms.fsm_old;

import com.google.common.collect.Maps;
import org.gradoop.flink.algorithms.fsm_old.common.canonicalization.api
  .CanonicalLabeler;
import org.gradoop.flink.algorithms.fsm_old.common.canonicalization.gspan
  .MinDFSLabeler;
import org.gradoop.flink.algorithms.fsm_old.common.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm_old.common.pojos.FSMEdge;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class MinDFSLabelerTest {

  @Test
  public void testDiamond() throws Exception {

    Map<Integer, String> vertices = Maps.newHashMap();

    vertices.put(0, "A");
    vertices.put(1, "A");
    vertices.put(2, "A");
    vertices.put(3, "A");

    Map<Integer, FSMEdge> edges = Maps.newHashMap();

    edges.put(0, new FSMEdge(0, "a", 1));
    edges.put(1, new FSMEdge(1, "a", 2));
    edges.put(2, new FSMEdge(0, "a", 3));
    edges.put(3, new FSMEdge(3, "a", 2));

    Embedding embedding = new Embedding(vertices, edges);

    CanonicalLabeler labeler = new MinDFSLabeler(true);

    String expectation = "0:A>a-1:A,1:A>a-2:A,2:A<a-3:A,3:A<a-0:A";
    String label = labeler.label(embedding);

    assertEquals(expectation, label);

    labeler = new MinDFSLabeler(false);

    expectation = "0:A-a-1:A,1:A-a-2:A,2:A-a-3:A,3:A-a-0:A";
    label = labeler.label(embedding);

    assertEquals(expectation, label);
  }

  @Test
  public void testMultiEdge() throws Exception {

    Map<Integer, String> vertices = Maps.newHashMap();

    vertices.put(0, "A");
    vertices.put(1, "A");
    vertices.put(2, "A");

    Map<Integer, FSMEdge> edges = Maps.newHashMap();

    edges.put(0, new FSMEdge(0, "a", 1));
    edges.put(1, new FSMEdge(0, "a", 1));
    edges.put(2, new FSMEdge(1, "a", 2));
    edges.put(3, new FSMEdge(1, "a", 2));

    Embedding embedding = new Embedding(vertices, edges);

    CanonicalLabeler labeler = new MinDFSLabeler(true);

    String expectation = "0:A>a-1:A,1:A<a-0:A,1:A>a-2:A,2:A<a-1:A";
    String label = labeler.label(embedding);

    assertEquals(expectation, label);

    labeler = new MinDFSLabeler(false);

    expectation = "0:A-a-1:A,1:A-a-0:A,1:A-a-2:A,2:A-a-1:A";
    label = labeler.label(embedding);

    assertEquals(expectation, label);
  }

  @Test
  public void testLoop() throws Exception {

    Map<Integer, String> vertices = Maps.newHashMap();

    vertices.put(0, "A");
    vertices.put(1, "A");
    vertices.put(2, "A");

    Map<Integer, FSMEdge> edges = Maps.newHashMap();

    edges.put(0, new FSMEdge(0, "a", 0));
    edges.put(1, new FSMEdge(0, "a", 1));
    edges.put(2, new FSMEdge(0, "a", 2));
    edges.put(3, new FSMEdge(1, "a", 2));

    Embedding embedding = new Embedding(vertices, edges);

    CanonicalLabeler labeler = new MinDFSLabeler(true);

    String expectation = "0:A>a-0:A,0:A>a-1:A,1:A>a-2:A,2:A<a-0:A";
    String label = labeler.label(embedding);

    assertEquals(expectation, label);

    labeler = new MinDFSLabeler(false);

    expectation = "0:A-a-0:A,0:A-a-1:A,1:A-a-2:A,2:A-a-0:A";
    label = labeler.label(embedding);

    assertEquals(expectation, label);
  }

  @Test
  public void testCircleWithBranch() throws Exception {

    Map<Integer, String> vertices = Maps.newHashMap();

    vertices.put(0, "A");
    vertices.put(1, "A");
    vertices.put(2, "A");
    vertices.put(3, "B");

    Map<Integer, FSMEdge> edges = Maps.newHashMap();

    edges.put(0, new FSMEdge(0, "a", 1));
    edges.put(1, new FSMEdge(1, "a", 2));
    edges.put(2, new FSMEdge(2, "a", 0));
    edges.put(3, new FSMEdge(2, "b", 3));

    Embedding embedding = new Embedding(vertices, edges);

    CanonicalLabeler labeler = new MinDFSLabeler(true);

    String expectation = "0:A>a-1:A,1:A>a-2:A,2:A>a-0:A,2:A>b-3:B";
    String label = labeler.label(embedding);

    assertEquals(expectation, label);

    labeler = new MinDFSLabeler(false);

    expectation = "0:A-a-1:A,1:A-a-2:A,2:A-a-0:A,2:A-b-3:B";
    label = labeler.label(embedding);

    assertEquals(expectation, label);
  }

  @Test
  public void testDirectedVsUndirected() throws Exception {

    Map<Integer, String> vertices = Maps.newHashMap();

    vertices.put(0, "A");
    vertices.put(1, "B");

    Map<Integer, FSMEdge> edges = Maps.newHashMap();

    edges.put(0, new FSMEdge(0, "b", 1));
    edges.put(1, new FSMEdge(1, "a", 0));


    Embedding embedding = new Embedding(vertices, edges);

    CanonicalLabeler labeler = new MinDFSLabeler(true);

    String expectation = "0:A>b-1:B,1:B>a-0:A";
    String label = labeler.label(embedding);

    assertEquals(expectation, label);

    labeler = new MinDFSLabeler(false);

    expectation = "0:A-a-1:B,1:B-b-0:A";
    label = labeler.label(embedding);

    assertEquals(expectation, label);
  }

  /**
   * Creation of paper examples
   */
//  @Test
//  public void test() {
//    Map<Integer, String> vertices = Maps.newHashMap();
//
//    vertices.put(0, "A");
//    vertices.put(1, "A");
//    vertices.put(2, "A");
//
//    Map<Integer, FSMEdge> edges = Maps.newHashMap();
//
//    edges.put(0, new FSMEdge(0, "a", 1));
//    edges.put(1, new FSMEdge(0, "a", 1));
//    edges.put(2, new FSMEdge(1, "a", 2));
//    edges.put(3, new FSMEdge(1, "a", 2));
//
//    Embedding embedding = new Embedding(vertices, edges);
//
//    System.out.println(new MinDFSLabeler(true).label(embedding));
//    System.out.println(new MinDFSLabeler(false).label(embedding));
//    System.out.println(new CAMLabeler(true).label(embedding));
//    System.out.println(new CAMLabeler(false).label(embedding));
//
//    vertices = Maps.newHashMap();
//
//    vertices.put(0, "A");
//    vertices.put(1, "B");
//
//    edges = Maps.newHashMap();
//
//    edges.put(0, new FSMEdge(0, "b", 1));
//    edges.put(1, new FSMEdge(0, "b", 1));
//    edges.put(2, new FSMEdge(1, "a", 0));
//
//    embedding = new Embedding(vertices, edges);
//
//    System.out.println(new MinDFSLabeler(true).label(embedding));
//    System.out.println(new MinDFSLabeler(false).label(embedding));
//    System.out.println(new CAMLabeler(true).label(embedding));
//    System.out.println(new CAMLabeler(false).label(embedding));
//  }
}