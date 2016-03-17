package es.upm.fi.dia.oeg.morph.base.query

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class AbstractQueryConditionTest {

    @Test def test_EqualOperator = {

        var cond1: AbstractQueryCondition = null
        var cond2: AbstractQueryCondition = null

        cond1 = new AbstractQueryConditionNotNull("ref1")
        cond2 = new AbstractQueryConditionNotNull("ref1")
        assertTrue(cond1 == cond2)

        cond1 = new AbstractQueryConditionNotNull("ref1")
        cond2 = new AbstractQueryConditionNotNull("ref2")
        assertFalse(cond1 == cond2)

        cond1 = new AbstractQueryConditionNotNull("ref1")
        cond2 = new AbstractQueryConditionIsNull("ref1")
        assertFalse(cond1 == cond2)

        cond1 = new AbstractQueryConditionIsNull("ref1")
        cond2 = new AbstractQueryConditionIsNull("ref1")
        assertTrue(cond1 == cond2)

        cond1 = new AbstractQueryConditionIsNull("ref1")
        cond2 = new AbstractQueryConditionIsNull("ref2")
        assertFalse(cond1 == cond2)

        cond1 = new AbstractQueryConditionIsNull("ref1")
        cond2 = new AbstractQueryConditionEquals("ref1", "toto")
        assertFalse(cond1 == cond2)

        cond1 = new AbstractQueryConditionEquals("ref1", "toto")
        cond2 = new AbstractQueryConditionEquals("ref1", "toto")
        assertTrue(cond1 == cond2)

        cond2 = new AbstractQueryConditionEquals("ref1", "toto")
        cond1 = new AbstractQueryConditionEquals("ref1", "tata")
        assertFalse(cond1 == cond2)

        cond1 = new AbstractQueryConditionEquals("ref1", "toto")
        cond2 = new AbstractQueryConditionEquals("ref2", "toto")
        assertFalse(cond1 == cond2)
    }

    @Test def test_EqualOperatorOr = {

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

        condOr1 = new AbstractQueryConditionOr(List(cond1))
        condOr2 = new AbstractQueryConditionOr(List(cond3))
        assertTrue(condOr1 == condOr2)

        condOr1 = new AbstractQueryConditionOr(List(cond1, cond2))
        condOr2 = new AbstractQueryConditionOr(List(cond3, cond4))
        assertTrue(condOr1 == condOr2)

        condOr1 = new AbstractQueryConditionOr(List(cond1, cond2))
        condOr2 = new AbstractQueryConditionOr(List(cond4, cond3))
        assertFalse(condOr1 == condOr2)

        condOr1 = new AbstractQueryConditionOr(List(cond1, cond2))
        condOr2 = new AbstractQueryConditionOr(List(cond3))
        assertFalse(condOr1 == condOr2)

        cond1 = new AbstractQueryConditionIsNull("ref1")
        cond2 = new AbstractQueryConditionEquals("ref2", "toto")
        cond3 = new AbstractQueryConditionIsNull("ref1")
        cond4 = new AbstractQueryConditionEquals("ref2", "toto")

        condOr1 = new AbstractQueryConditionOr(List(cond1, cond2))
        condOr2 = new AbstractQueryConditionOr(List(cond3, cond4))
        assertTrue(condOr1 == condOr2)

        condOr1 = new AbstractQueryConditionOr(List(cond1, cond2))
        condOr2 = new AbstractQueryConditionOr(List(cond4, cond3))
        assertFalse(condOr1 == condOr2)
    }
}