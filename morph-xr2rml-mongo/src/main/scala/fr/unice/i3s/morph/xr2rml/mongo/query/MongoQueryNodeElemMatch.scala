package fr.unice.i3s.morph.xr2rml.mongo.query

import scala.collection.mutable.Queue

/**
 * MongoDB query starting with an elemMath node:
 * 	"$elemMatch: {...}"
 */
class MongoQueryNodeElemMatch(val members: List[MongoQueryNode]) extends MongoQueryNode {

    /** Constructor with only one member */
    def this(member: MongoQueryNode) = this(List(member))

    override def isElemMatch: Boolean = true

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[MongoQueryNodeElemMatch] && this.members == q.asInstanceOf[MongoQueryNodeElemMatch].members
    }

    override def toString() = { "$elemMatch: {" + members.mkString(", ") + "}" }

    /**
     * Pull up members of an AND as members of the ELEMMATCH
     */
    def flattenAnds: MongoQueryNodeElemMatch = {
        val optMembers = new Queue[MongoQueryNode]
        for (m <- members) {
            if (m.isAnd)
                optMembers ++= m.asInstanceOf[MongoQueryNodeAnd].members
            else
                optMembers += m
        }

        val optList = optMembers.toList
        if (optList != members) {
            if (logger.isTraceEnabled())
                logger.trace("Optimized [" + members + "] into [" + optList + "]")
            new MongoQueryNodeElemMatch(optList)
        } else this
    }
}
