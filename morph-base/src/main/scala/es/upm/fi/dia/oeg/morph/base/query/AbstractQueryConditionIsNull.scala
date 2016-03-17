package es.upm.fi.dia.oeg.morph.base.query

/**
 * Is-Null condition of the <i>Where</i> part an atomic abstract query, created during the rewriting
 * process by matching terms of a triple pattern with references from a term map.
 *
 * @param reference the xR2RML reference (e.g. column name or JSONPath expression) on which the condition applies
 * @author Franck Michel (franck.michel@cnrs.fr)
 */
class AbstractQueryConditionIsNull(
        val reference: String) extends AbstractQueryCondition(ConditionType.IsNull) with IReference {

    override def toString: String = {
        "IsNull(" + reference + ")"
    }

    override def equals(c: Any): Boolean = {
        c.isInstanceOf[AbstractQueryConditionIsNull] &&
            this.reference == c.asInstanceOf[AbstractQueryConditionIsNull].reference
    }
}
