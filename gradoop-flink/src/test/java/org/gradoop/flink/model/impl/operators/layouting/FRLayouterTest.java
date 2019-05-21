package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class FRLayouterTest extends LayoutingAlgorithmTest {

    @Test
    public void TestCellIdSelector() throws Exception {
        FRLayouter frl = new FRLayouter(1, 1, 100, 100, 10);
        KeySelector<Vertex, Integer> selfselector = frl.getCellIdSelector(FRLayouter.NeighborType.SELF);

        Assert.assertEquals(0, (long) selfselector.getKey(getDummyVertex(0, 0, frl)));
        Assert.assertEquals(99, (long) selfselector.getKey(getDummyVertex(99, 98, frl)));
        Assert.assertEquals(90, (long) selfselector.getKey(getDummyVertex(0, 95, frl)));

        KeySelector<Vertex, Integer> neighborslector = frl.getCellIdSelector(FRLayouter.NeighborType.RIGHT);
        Assert.assertEquals(01, (long) neighborslector.getKey(getDummyVertex(0)));
        Assert.assertEquals(36, (long) neighborslector.getKey(getDummyVertex(35)));
        Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(99)));

        neighborslector = frl.getCellIdSelector(FRLayouter.NeighborType.LEFT);
        Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(0)));
        Assert.assertEquals(34, (long) neighborslector.getKey(getDummyVertex(35)));
        Assert.assertEquals(98, (long) neighborslector.getKey(getDummyVertex(99)));

        neighborslector = frl.getCellIdSelector(FRLayouter.NeighborType.UP);
        Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(0)));
        Assert.assertEquals(25, (long) neighborslector.getKey(getDummyVertex(35)));
        Assert.assertEquals(89, (long) neighborslector.getKey(getDummyVertex(99)));

        neighborslector = frl.getCellIdSelector(FRLayouter.NeighborType.DOWN);
        Assert.assertEquals(10, (long) neighborslector.getKey(getDummyVertex(0)));
        Assert.assertEquals(45, (long) neighborslector.getKey(getDummyVertex(35)));
        Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(99)));

        neighborslector = frl.getCellIdSelector(FRLayouter.NeighborType.UPRIGHT);
        Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(0)));
        Assert.assertEquals(26, (long) neighborslector.getKey(getDummyVertex(35)));
        Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(99)));

        neighborslector = frl.getCellIdSelector(FRLayouter.NeighborType.UPLEFT);
        Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(0)));
        Assert.assertEquals(24, (long) neighborslector.getKey(getDummyVertex(35)));
        Assert.assertEquals(88, (long) neighborslector.getKey(getDummyVertex(99)));

        neighborslector = frl.getCellIdSelector(FRLayouter.NeighborType.DOWNLEFT);
        Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(0)));
        Assert.assertEquals(44, (long) neighborslector.getKey(getDummyVertex(35)));
        Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(99)));

        neighborslector = frl.getCellIdSelector(FRLayouter.NeighborType.DOWNRIGHT);
        Assert.assertEquals(11, (long) neighborslector.getKey(getDummyVertex(0)));
        Assert.assertEquals(46, (long) neighborslector.getKey(getDummyVertex(35)));
        Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(99)));

    }

    @Test
    public void TestRepulseJoinFunction() throws Exception {
        FRLayouter frl = new FRLayouter(1, 1, 100, 100, 10);
        JoinFunction<Vertex, Vertex, Tuple3<GradoopId, Double, Double>> jf = frl.getRepulsionJoinFunction();
        Vertex v1 = getDummyVertex(1, 1, frl);
        Vertex v2 = getDummyVertex(2, 3, frl);
        Vertex v3 = getDummyVertex(7, 5, frl);
        Vertex v4 = getDummyVertex(1, 1, frl);

        Vector vec12 = Vector.fromForceTuple(jf.join(v1, v2));
        Vector vec13 = Vector.fromForceTuple(jf.join(v1, v3));
        Vector vec14 = Vector.fromForceTuple(jf.join(v1, v4));
        Vector vec11 = Vector.fromForceTuple(jf.join(v1, v1));

        Assert.assertTrue(vec12.magnitude() > vec13.magnitude());
        Assert.assertTrue(vec14.magnitude() > 0);
        Assert.assertTrue(vec11.magnitude() == 0);
    }

    private Vertex getDummyVertex(int cellid) {
        Vertex v = new Vertex(new GradoopId(), "testlabel", new Properties(), null);
        v.setProperty(FRLayouter.CELLID_PROPERTY, new Integer(cellid));
        return v;
    }

    private Vertex getDummyVertex(int x, int y, FRLayouter frl) throws Exception {
        MapFunction<Vertex, Vertex> idmapper = frl.getIdMapperFunction();
        Vertex v = new Vertex(GradoopId.get(), "testlabel", new Properties(), null);
        Vector pos = new Vector(x, y);
        pos.setVertexPosition(v);
        v = idmapper.map(v);
        return v;
    }

    @Override
    public LayoutingAlgorithm getLayouter(int w, int h) {
        return new FRLayouter(FRLayouter.calculateK(w, h, 10), 5, w, h,4);
    }
}
