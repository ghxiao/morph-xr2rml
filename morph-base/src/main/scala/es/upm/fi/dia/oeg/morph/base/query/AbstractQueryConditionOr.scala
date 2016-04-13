package es.upm.fi.dia.oeg.morph.base.query

import scala.annotation.migration

/**
 * OR condition of the <i>Where</i> part an atomic abstract query created during the
 * abstract query optimization phase.
 *
 * @param members the member conditions of the OR
 * 
 * @author Franck Michel, I3S laboratory
 */
class AbstractQueryConditionOr(
        val members: Set[AbstractQueryCondition]) extends AbstractQueryCondition(ConditionType.Or) {

    override def hasReference = false

    override def toString: String = {
        "Or(" + members.mkString(", ") + ")"
    }

    override def equals(c: Any): Boolean = {
        c.isInstanceOf[AbstractQueryConditionOr] &&
            this.members == c.asInstanceOf[AbstractQueryConditionOr].members
    }

    override def hashCode(): Int = {
        this.getClass.hashCode + this.members.map(_.hashCode).reduceLeft((x, y) => x + y)
    }
}

object AbstractQueryConditionOr {

    /**
     * Constructor that flattens nested ORs, and in case only one element remains
     * it is returned instead of creating an Or of one member.
     *
     */
    def create(lstMembers: Set[AbstractQueryCondition]): AbstractQueryCondition = {

        // Flatten nested ORs
        var flatMembers = Set[AbstractQueryCondition]()
        lstMembers.foreach { c =>
            if (c.condType == ConditionType.Or)
                flatMembers ++= (c.asInstanceOf[AbstractQueryConditionOr].members)
            else
                flatMembers += c
        }

        if (flatMembers.size == 1)
            flatMembers.head
        else
            new AbstractQueryConditionOr(flatMembers)
    }
}