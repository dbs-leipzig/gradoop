package org.gradoop.flink.model.impl.functions.epgm.filters;

import org.junit.Test;

import static org.junit.Assert.*;

public class CombineableFilterTest {

    private CombineableFilter<Object> alwaysTrue = e -> true;

    private CombineableFilter<Object> alwaysFalse = e -> false;

    private Object testObject = new Object();

    @Test
    public void testAnd() throws Exception {
        assertTrue(alwaysTrue.and(alwaysTrue).filter(testObject));
        assertFalse(alwaysFalse.and(alwaysTrue).filter(testObject));
        assertFalse(alwaysTrue.and(alwaysFalse).filter(testObject));
        assertFalse(alwaysFalse.and(alwaysFalse).filter(testObject));
    }

    @Test
    public void testOr() throws Exception {
        assertTrue(alwaysTrue.or(alwaysTrue).filter(testObject));
        assertTrue(alwaysFalse.or(alwaysTrue).filter(testObject));
        assertTrue(alwaysTrue.or(alwaysFalse).filter(testObject));
        assertFalse(alwaysFalse.or(alwaysFalse).filter(testObject));
    }

    @Test
    public void testNegate() throws Exception {
        assertTrue(alwaysFalse.negate().filter(testObject));
        assertFalse(alwaysTrue.negate().filter(testObject));
    }

}
