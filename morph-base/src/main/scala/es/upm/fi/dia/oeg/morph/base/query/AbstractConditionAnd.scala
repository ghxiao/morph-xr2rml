package es.upm.fi.dia.oeg.morph.base.query

import scala.annotation.migration

/**
 * AND condition of the <i>Where</i> part an atomic abstract query created during the
 * abstract query optimization phase.
 *
 * @param members the member conditions of the AND
 * 
 * @author Franck Michel, I3S laboratory
 */
class AbstractConditionAnd(
        val members: Set[AbstractCondition]) extends AbstractCondition(ConditionType.And) {

    override def hasReference = false

    override def toString: String = {
        "And(" + members.mkString(", ") + ")"
    }

    override def equals(c: Any): Boolean = {
        c.isInstanceOf[AbstractConditionAnd] &&
            this.members == c.asInstanceOf[AbstractConditionAnd].members
    }

    override def hashCode(): Int = {
        this.members.map { x => x.hashCode }.sum
    }
}

object AbstractConditionAnd {

    /**
     * Constructor that avoids to create an equality and not-null condition on the same reference like
     * And(Equals(ref, val), NotNull(ref), instead only the equality is kept.
     *
     * Nested Ands are flattened, and in case only one element remains it is returned instead of creating an
     * And of one member.
     */
    def create(lstMembers: Set[AbstractCondition]): AbstractCondition = {

        // Flatten ANDs
        var flatMembers = Set[AbstractCondition]()
        lstMembers.foreach { c =>
            if (c.condType == ConditionType.And)
                flatMembers ++= c.asInstanceOf[AbstractConditionAnd].members
            else
                flatMembers += c
        }

        // Simplification And(Equals(ref, value), NotNull(ref)) => Equals(ref, value)
        var _lstMmbrs = flatMembers
        if (flatMembers.size == 2) {
            val elem1 = flatMembers.head
            val elem2 = flatMembers.tail.head
            if (elem1.hasReference && elem2.hasReference) {
                val elem1Ref = elem1.asInstanceOf[IReference]
                val elem2Ref = elem2.asInstanceOf[IReference]
                if (elem1Ref.reference == elem2Ref.reference) {
                    if (elem1Ref.isInstanceOf[AbstractConditionEquals] && elem2Ref.isInstanceOf[AbstractConditionNotNull])
                        _lstMmbrs = Set(elem1)
                    else if (elem1Ref.isInstanceOf[AbstractConditionNotNull] && elem2Ref.isInstanceOf[AbstractConditionEquals])
                        _lstMmbrs = Set(elem2)
                }
            }
        }

        if (_lstMmbrs.size == 1)
            _lstMmbrs.head
        else
            new AbstractConditionAnd(flatMembers)
    }
}
