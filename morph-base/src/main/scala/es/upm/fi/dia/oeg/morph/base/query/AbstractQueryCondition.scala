package es.upm.fi.dia.oeg.morph.base.query

import es.upm.fi.dia.oeg.morph.base.exception.MorphException

/**
 * Condition of the <i>Where</i> part of an atomic abstract query, created during the
 * query rewriting process.
 *
 * Initially, only two types of condition can come up during by matching terms of a
 * triple pattern with the corresponding references from a term map: Equals and IsNotNull.
 *
 * Then, during the abstract query optimization, some new types of condition may come up:
 *
 * (i) In a self-union elimination, conditions of two atomic queries Q1 and Q2 are merged,
 * and the new Where condition is OR(Q1.where, Q2.where). But when there are more than
 * one condition in each Where, we get OR(AND(Q1.where conditions), AND(Q2.where conditions)).
 *
 * (ii) In optional-join elimination (left-join), the Equals conditions of the right query
 * are relaxed since null is allowed in the right part:
 * Equals(reference, value) becomes OR(Equals(reference, value), IsNull(reference))
 *
 * @param condType The type of condition
 * 
 * @author Franck Michel, I3S laboratory
 */
abstract class AbstractQueryCondition(val condType: ConditionType.Value) {

    /**
     * Return true for a condition that has a reference, that is Equals, IsNotNull and IsNull
     */
    def hasReference: Boolean

    override def equals(c: Any): Boolean = {
        throw new MorphException("Method not implemented.")
    }
}

/**
 * Types of conditions in the <i>Where</i> part an atomic abstract query.
 */
object ConditionType extends Enumeration {
    val Equals, IsNotNull, IsNull, Or, And, SparqlFilter = Value
}
