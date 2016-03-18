package es.upm.fi.dia.oeg.morph.base.query

/**
 * OR condition of the <i>Where</i> part an atomic abstract query created during the
 * abstract query optimization phase.
 *
 * @param members the member conditions of the OR
 */
class AbstractQueryConditionOr(
        val members: List[AbstractQueryCondition]) extends AbstractQueryCondition(ConditionType.Or) {

    override def hasReference = false

    override def toString: String = {
        "Or(" + members.mkString(", ") + ")"
    }

    override def equals(c: Any): Boolean = {
        c.isInstanceOf[AbstractQueryConditionOr] &&
            this.members == c.asInstanceOf[AbstractQueryConditionOr].members
    }
}
