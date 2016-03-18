package es.upm.fi.dia.oeg.morph.base.query

import es.upm.fi.dia.oeg.morph.base.exception.MorphException

/**
 * Condition of the <i>Where</i> part an atomic abstract query, created during the rewriting
 * process by matching terms of a triple pattern with references from a term map.
 *
 * @param condType The type of condition
 */
abstract class AbstractQueryCondition(
        val condType: ConditionType.Value) {
    
    def hasReference: Boolean
}

/**
 * Types of conditions in the <i>Where</i> part an atomic abstract query.
 */
object ConditionType extends Enumeration {
    val Equals, IsNotNull, IsNull, Or, SparqlFilter = Value
}