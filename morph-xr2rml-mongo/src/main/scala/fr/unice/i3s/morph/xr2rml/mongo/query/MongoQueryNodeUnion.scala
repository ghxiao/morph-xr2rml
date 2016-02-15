package fr.unice.i3s.morph.xr2rml.mongo.query

import scala.collection.mutable.Queue

/**
 * A UNION of MongoDB queries. This is semantically equivalent to an OR node,
 * but will be executed a separate queries whereof results are joined by the xR2RML processing engine
 */
class MongoQueryNodeUnion(val members: List[MongoQueryNode]) extends MongoQueryNode {

    override def isUnion: Boolean = true

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[MongoQueryNodeUnion] && this.members == q.asInstanceOf[MongoQueryNodeUnion].members
    }

    def this(member1: MongoQueryNode, member2: MongoQueryNode) = this(List(member1, member2))

    /**
     * This method is for debug purpose only since it returns a string that is not a valid MongoDB query
     */
    override def toString() = { "UNION: [{" + members.mkString("}, {") + "}]" }

    /**
     * Group WHEREs:
     * 	UNION(..., WHERE(W1), WHERE(W2)) => UNION(..., WHERE(W1 || W2)
     */
    def groupWheres: MongoQueryNodeUnion = {
        // Split WHERE nodes and other nodes
        val (wheres, others) = members.partition(_.isWhere)

        if (wheres.length <= 1)
            this // no where condition or just one => no change
        else {
            val optMembers = {
                var isFirst = true
                var groupedWhereConds = ""
                for (w <- wheres) {
                    val whereCond = w.asInstanceOf[MongoQueryNodeWhere].cond
                    if (isFirst) {
                        groupedWhereConds = "(" + whereCond + ")"
                        isFirst = false
                    } else
                        groupedWhereConds += " || (" + whereCond + ")"
                }
                // Create a new list of nodes where the multiple WHERE nodes are replaced by a single one 
                (others :+ new MongoQueryNodeWhere(groupedWhereConds)).toList
            }

            if (optMembers != members) {
                if (logger.isTraceEnabled())
                    logger.trace("Optimized [" + members + "] into [" + optMembers + "]")
                new MongoQueryNodeUnion(optMembers)
            } else this
        }
    }

    /**
     * Flatten several nested UNIONs into a single one
     */
    def flattenUnions: MongoQueryNodeUnion = {
        val optMembers = new Queue[MongoQueryNode]
        for (m <- members) {
            if (m.isUnion)
                optMembers ++= m.asInstanceOf[MongoQueryNodeUnion].members
            else
                optMembers += m
        }

        val optList = optMembers.toList
        if (optList != members) {
            if (logger.isTraceEnabled())
                logger.trace("Optimized [" + members + "] into [" + optList + "]")
            new MongoQueryNodeUnion(optList)
        } else this
    }

}
