package es.upm.fi.dia.oeg.morph.base.query

/**
 * Is-Null condition of the <i>Where</i> part an atomic abstract query, created during the
 * abstract query optimization phase by the optional-join elimination.
 *
 * @param reference the xR2RML reference (e.g. column name or JSONPath expression) on which the condition applies
 */
class AbstractQueryConditionIsNull(
        var reference: String) extends AbstractQueryCondition(ConditionType.IsNull) with IReference {

    override def toString: String = {
        "IsNull(" + reference + ")"
    }

    override def equals(c: Any): Boolean = {
        c.isInstanceOf[AbstractQueryConditionIsNull] &&
            this.reference == c.asInstanceOf[AbstractQueryConditionIsNull].reference
    }
}
