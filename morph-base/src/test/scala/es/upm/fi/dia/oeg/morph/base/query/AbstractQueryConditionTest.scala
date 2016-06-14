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

        var cond1: AbstractCondition = null
        var cond2: AbstractCondition = null

        cond1 = new AbstractConditionNotNull("ref1")
        cond2 = new AbstractConditionNotNull("ref1")
        assertTrue(cond1 == cond2)
        assertTrue(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractConditionNotNull("ref1")
        cond2 = new AbstractConditionNotNull("ref2")
        assertFalse(cond1 == cond2)
        assertFalse(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractConditionNotNull("ref1")
        cond2 = new AbstractConditionIsNull("ref1")
        assertFalse(cond1 == cond2)
        assertFalse(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractConditionIsNull("ref1")
        cond2 = new AbstractConditionIsNull("ref1")
        assertTrue(cond1 == cond2)
        assertTrue(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractConditionIsNull("ref1")
        cond2 = new AbstractConditionIsNull("ref2")
        assertFalse(cond1 == cond2)
        assertFalse(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractConditionIsNull("ref1")
        cond2 = new AbstractConditionEquals("ref1", "toto")
        assertFalse(cond1 == cond2)
        assertFalse(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractConditionEquals("ref1", "toto")
        cond2 = new AbstractConditionEquals("ref1", "toto")
        assertTrue(cond1 == cond2)
        assertTrue(cond1.hashCode == cond2.hashCode)

        cond2 = new AbstractConditionEquals("ref1", "toto")
        cond1 = new AbstractConditionEquals("ref1", "tata")
        assertFalse(cond1 == cond2)
        assertFalse(cond1.hashCode == cond2.hashCode)

        cond1 = new AbstractConditionEquals("ref1", "toto")
        cond2 = new AbstractConditionEquals("ref2", "toto")
        assertFalse(cond1 == cond2)
    }

    @Test def test_EqualOperator2 = {

        var cond1: AbstractCondition = null
        var cond2: AbstractCondition = null
        var cond3: AbstractCondition = null
        var cond4: AbstractCondition = null
        var condOr1: AbstractCondition = null
        var condOr2: AbstractCondition = null

        cond1 = new AbstractConditionNotNull("ref1")
        cond2 = new AbstractConditionNotNull("ref2")
        cond3 = new AbstractConditionNotNull("ref1")
        cond4 = new AbstractConditionNotNull("ref2")

        condOr1 = AbstractConditionOr.create(Set(cond1))
        condOr2 = AbstractConditionOr.create(Set(cond3))
        assertTrue(condOr1 == condOr2)
        assertTrue(condOr1.hashCode == condOr2.hashCode)

        condOr1 = AbstractConditionOr.create(Set(cond1, cond2))
        condOr2 = AbstractConditionOr.create(Set(cond4, cond3))
        assertTrue(condOr1 == condOr2)
        assertTrue(condOr1.hashCode == condOr2.hashCode)

        condOr1 = AbstractConditionOr.create(Set(cond1, cond2))
        condOr2 = AbstractConditionOr.create(Set(cond3))
        assertFalse(condOr1 == condOr2)
        assertFalse(condOr1.hashCode == condOr2.hashCode)

        cond1 = new AbstractConditionIsNull("ref1")
        cond2 = new AbstractConditionEquals("ref2", "toto")
        cond3 = new AbstractConditionIsNull("ref1")
        cond4 = new AbstractConditionEquals("ref2", "toto")

        condOr1 = AbstractConditionOr.create(Set(cond1, cond2))
        condOr2 = AbstractConditionOr.create(Set(cond3, cond4))
        assertTrue(condOr1 == condOr2)
        assertTrue(condOr1.hashCode == condOr2.hashCode)

        val condAnd1 = AbstractConditionAnd.create(Set(cond1, cond2))
        val condAnd2 = AbstractConditionAnd.create(Set(cond3, cond4))
        assertTrue(condAnd1 == condAnd2)
        assertTrue(condAnd1.hashCode == condAnd2.hashCode)
    }

    @Test def test_EqualOperator3 = {
        val cond1 = AbstractConditionAnd.create(
            Set(new AbstractConditionEquals("ref1", new Integer(60585)),
                new AbstractConditionNotNull("ref1")))

        val cond2 = AbstractConditionAnd.create(
            Set(new AbstractConditionNotNull("ref1"),
                new AbstractConditionEquals("ref1", new Integer(60585))))

        val cond3 = AbstractConditionAnd.create(
            Set(new AbstractConditionEquals("ref1", new Integer(60585)),
                new AbstractConditionNotNull("ref2")))

        assertTrue(cond1 == cond2)
        assertTrue(cond1.hashCode == cond2.hashCode)

        assertFalse(cond1 == cond3)
        assertFalse(cond1.hashCode == cond3.hashCode)

        val or1 = AbstractConditionOr.create(Set(cond1, cond3))
        val or2 = AbstractConditionOr.create(Set(cond2, cond3))
        println(or1)
        println(or2)
        assertTrue(or1 == or2)
        assertTrue(or1.hashCode == or2.hashCode)

        val and1 = AbstractConditionAnd.create(Set(cond1, cond3))
        val and2 = AbstractConditionAnd.create(Set(cond2, cond3))
        assertTrue(and1 == and2)
        assertTrue(and1.hashCode == and2.hashCode)
    }

    @Test def test_FlattenOr = {
        val cond1 = AbstractConditionAnd.create(
            Set(new AbstractConditionEquals("ref1", new Integer(60585)), new AbstractConditionNotNull("ref1")))

        val cond2 = AbstractConditionAnd.create(
            Set(new AbstractConditionNotNull("ref1"), new AbstractConditionEquals("ref1", new Integer(60585))))

        val cond3 = AbstractConditionAnd.create(
            Set(new AbstractConditionEquals("ref1", new Integer(60585)), new AbstractConditionNotNull("ref2")))

        // Flattened ORs
        val or1 = AbstractConditionOr.create(Set(cond1, cond3))
        val or2 = AbstractConditionOr.create(Set(cond2, cond3))
        val or = AbstractConditionOr.create(Set(or1, or2))
        println(or)
        assertTrue(or.asInstanceOf[AbstractConditionOr].members.size == 2)
        assertTrue(or.asInstanceOf[AbstractConditionOr].members.contains(cond1))
        assertTrue(or.asInstanceOf[AbstractConditionOr].members.contains(cond2))
        assertTrue(or.asInstanceOf[AbstractConditionOr].members.contains(cond3))
    }

    @Test def test_SimplifyAnd = {
        val cond1 = AbstractConditionAnd.create(
            Set(new AbstractConditionEquals("ref1", new Integer(60585)),
                new AbstractConditionNotNull("ref1")))

        assertTrue(cond1.isInstanceOf[AbstractConditionEquals])

        val cond2 = AbstractConditionAnd.create(
            Set(new AbstractConditionNotNull("ref1"),
                new AbstractConditionEquals("ref2", new Integer(60585))))

        assertTrue(cond2.isInstanceOf[AbstractConditionAnd])
        assertEquals(2, cond2.asInstanceOf[AbstractConditionAnd].members.size)
    }

    @Test def test_FlattenAnd = {
        val cond1 = AbstractConditionOr.create(
            Set(new AbstractConditionEquals("ref1", new Integer(60585)), new AbstractConditionNotNull("ref1")))

        val cond2 = AbstractConditionOr.create(
            Set(new AbstractConditionNotNull("ref1"), new AbstractConditionEquals("ref1", new Integer(60585))))

        val cond3 = AbstractConditionOr.create(
            Set(new AbstractConditionEquals("ref1", new Integer(60585)), new AbstractConditionNotNull("ref2")))

        // Flattened ORs
        val and1 = AbstractConditionAnd.create(Set(cond1, cond3))
        val and2 = AbstractConditionAnd.create(Set(cond2, cond3))
        val and = AbstractConditionAnd.create(Set(and1, and2))
        println(and)
        assertTrue(and.asInstanceOf[AbstractConditionAnd].members.size == 2)
        assertTrue(and.asInstanceOf[AbstractConditionAnd].members.contains(cond1))
        assertTrue(and.asInstanceOf[AbstractConditionAnd].members.contains(cond2))
        assertTrue(and.asInstanceOf[AbstractConditionAnd].members.contains(cond3))
    }

}