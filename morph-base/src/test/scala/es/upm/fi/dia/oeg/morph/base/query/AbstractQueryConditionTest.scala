package es.upm.fi.dia.oeg.morph.base.query

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * @author Franck Michel, I3S laboratory
 *
 */
class AbstractQueryConditionTest {

    @Test def test_EqualOperator = {

        var cond1: AbstractQueryCondition = null
        var cond2: AbstractQueryCondition = null

        cond1 = new AbstractQueryConditionNotNull("ref1")
        cond2 = new AbstractQueryConditionNotNull("ref1")
        assertTrue(cond1 == cond2)
        assertTrue(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractQueryConditionNotNull("ref1")
        cond2 = new AbstractQueryConditionNotNull("ref2")
        assertFalse(cond1 == cond2)
        assertFalse(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractQueryConditionNotNull("ref1")
        cond2 = new AbstractQueryConditionIsNull("ref1")
        assertFalse(cond1 == cond2)
        assertFalse(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractQueryConditionIsNull("ref1")
        cond2 = new AbstractQueryConditionIsNull("ref1")
        assertTrue(cond1 == cond2)
        assertTrue(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractQueryConditionIsNull("ref1")
        cond2 = new AbstractQueryConditionIsNull("ref2")
        assertFalse(cond1 == cond2)
        assertFalse(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractQueryConditionIsNull("ref1")
        cond2 = new AbstractQueryConditionEquals("ref1", "toto")
        assertFalse(cond1 == cond2)
        assertFalse(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractQueryConditionEquals("ref1", "toto")
        cond2 = new AbstractQueryConditionEquals("ref1", "toto")
        assertTrue(cond1 == cond2)
        assertTrue(cond1.hashCode == cond2.hashCode)

        cond2 = new AbstractQueryConditionEquals("ref1", "toto")
        cond1 = new AbstractQueryConditionEquals("ref1", "tata")
        assertFalse(cond1 == cond2)
        assertFalse(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractQueryConditionEquals("ref1", "toto")
        cond2 = new AbstractQueryConditionEquals("ref2", "toto")
        assertFalse(cond1 == cond2)
    }

    @Test def test_EqualOperator2 = {

        var cond1: AbstractQueryCondition = null
        var cond2: AbstractQueryCondition = null
        var cond3: AbstractQueryCondition = null
        var cond4: AbstractQueryCondition = null
        var condOr1: AbstractQueryCondition = null
        var condOr2: AbstractQueryCondition = null

        cond1 = new AbstractQueryConditionNotNull("ref1")
        cond2 = new AbstractQueryConditionNotNull("ref2")
        cond3 = new AbstractQueryConditionNotNull("ref1")
        cond4 = new AbstractQueryConditionNotNull("ref2")

        condOr1 = AbstractQueryConditionOr.create(Set(cond1))
        condOr2 = AbstractQueryConditionOr.create(Set(cond3))
        assertTrue(condOr1 == condOr2)
        assertTrue(condOr1.hashCode == condOr2.hashCode)

        condOr1 = AbstractQueryConditionOr.create(Set(cond1, cond2))
        condOr2 = AbstractQueryConditionOr.create(Set(cond4, cond3))
        assertTrue(condOr1 == condOr2)
        assertTrue(condOr1.hashCode == condOr2.hashCode)

        condOr1 = AbstractQueryConditionOr.create(Set(cond1, cond2))
        condOr2 = AbstractQueryConditionOr.create(Set(cond3))
        assertFalse(condOr1 == condOr2)
        assertFalse(condOr1.hashCode == condOr2.hashCode)

        cond1 = new AbstractQueryConditionIsNull("ref1")
        cond2 = new AbstractQueryConditionEquals("ref2", "toto")
        cond3 = new AbstractQueryConditionIsNull("ref1")
        cond4 = new AbstractQueryConditionEquals("ref2", "toto")

        condOr1 = AbstractQueryConditionOr.create(Set(cond1, cond2))
        condOr2 = AbstractQueryConditionOr.create(Set(cond3, cond4))
        assertTrue(condOr1 == condOr2)
        assertTrue(condOr1.hashCode == condOr2.hashCode)

        val condAnd1 = AbstractQueryConditionAnd.create(Set(cond1, cond2))
        val condAnd2 = AbstractQueryConditionAnd.create(Set(cond3, cond4))
        assertTrue(condAnd1 == condAnd2)
        assertTrue(condAnd1.hashCode == condAnd2.hashCode)
    }

    @Test def test_EqualOperator3 = {
        val cond1 = AbstractQueryConditionAnd.create(
            Set(new AbstractQueryConditionEquals("ref1", new Integer(60585)),
                new AbstractQueryConditionNotNull("ref1")))

        val cond2 = AbstractQueryConditionAnd.create(
            Set(new AbstractQueryConditionNotNull("ref1"),
                new AbstractQueryConditionEquals("ref1", new Integer(60585))))

        val cond3 = AbstractQueryConditionAnd.create(
            Set(new AbstractQueryConditionEquals("ref1", new Integer(60585)),
                new AbstractQueryConditionNotNull("ref2")))

        assertTrue(cond1 == cond2)
        assertTrue(cond1.hashCode == cond2.hashCode)

        assertFalse(cond1 == cond3)
        assertFalse(cond1.hashCode == cond3.hashCode)

        val or1 = AbstractQueryConditionOr.create(Set(cond1, cond3))
        val or2 = AbstractQueryConditionOr.create(Set(cond2, cond3))
        println(or1)
        println(or2)
        assertTrue(or1 == or2)
        assertTrue(or1.hashCode == or2.hashCode)

        val and1 = AbstractQueryConditionAnd.create(Set(cond1, cond3))
        val and2 = AbstractQueryConditionAnd.create(Set(cond2, cond3))
        assertTrue(and1 == and2)
        assertTrue(and1.hashCode == and2.hashCode)
    }

    @Test def test_FlattenOr = {
        val cond1 = AbstractQueryConditionAnd.create(
            Set(new AbstractQueryConditionEquals("ref1", new Integer(60585)), new AbstractQueryConditionNotNull("ref1")))

        val cond2 = AbstractQueryConditionAnd.create(
            Set(new AbstractQueryConditionNotNull("ref1"), new AbstractQueryConditionEquals("ref1", new Integer(60585))))

        val cond3 = AbstractQueryConditionAnd.create(
            Set(new AbstractQueryConditionEquals("ref1", new Integer(60585)), new AbstractQueryConditionNotNull("ref2")))

        // Flattened ORs
        val or1 = AbstractQueryConditionOr.create(Set(cond1, cond3))
        val or2 = AbstractQueryConditionOr.create(Set(cond2, cond3))
        val or = AbstractQueryConditionOr.create(Set(or1, or2))
        println(or)
        assertTrue(or.asInstanceOf[AbstractQueryConditionOr].members.size == 2)
        assertTrue(or.asInstanceOf[AbstractQueryConditionOr].members.contains(cond1))
        assertTrue(or.asInstanceOf[AbstractQueryConditionOr].members.contains(cond2))
        assertTrue(or.asInstanceOf[AbstractQueryConditionOr].members.contains(cond3))
    }

    @Test def test_SimplifyAnd = {
        val cond1 = AbstractQueryConditionAnd.create(
            Set(new AbstractQueryConditionEquals("ref1", new Integer(60585)),
                new AbstractQueryConditionNotNull("ref1")))

        assertTrue(cond1.isInstanceOf[AbstractQueryConditionEquals])

        val cond2 = AbstractQueryConditionAnd.create(
            Set(new AbstractQueryConditionNotNull("ref1"),
                new AbstractQueryConditionEquals("ref2", new Integer(60585))))

        assertTrue(cond2.isInstanceOf[AbstractQueryConditionAnd])
        assertEquals(2, cond2.asInstanceOf[AbstractQueryConditionAnd].members.size)
    }

    @Test def test_FlattenAnd = {
        val cond1 = AbstractQueryConditionOr.create(
            Set(new AbstractQueryConditionEquals("ref1", new Integer(60585)), new AbstractQueryConditionNotNull("ref1")))

        val cond2 = AbstractQueryConditionOr.create(
            Set(new AbstractQueryConditionNotNull("ref1"), new AbstractQueryConditionEquals("ref1", new Integer(60585))))

        val cond3 = AbstractQueryConditionOr.create(
            Set(new AbstractQueryConditionEquals("ref1", new Integer(60585)), new AbstractQueryConditionNotNull("ref2")))

        // Flattened ORs
        val and1 = AbstractQueryConditionAnd.create(Set(cond1, cond3))
        val and2 = AbstractQueryConditionAnd.create(Set(cond2, cond3))
        val and = AbstractQueryConditionAnd.create(Set(and1, and2))
        println(and)
        assertTrue(and.asInstanceOf[AbstractQueryConditionAnd].members.size == 2)
        assertTrue(and.asInstanceOf[AbstractQueryConditionAnd].members.contains(cond1))
        assertTrue(and.asInstanceOf[AbstractQueryConditionAnd].members.contains(cond2))
        assertTrue(and.asInstanceOf[AbstractQueryConditionAnd].members.contains(cond3))
    }

}