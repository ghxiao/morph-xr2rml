package fr.unice.i3s.morph.xr2rml.mongo.query

import scala.collection.mutable.Queue

/**
 * MongoDB query starting with an OR node:
 * 	$or: [{doc1}, {doc1}... {docN}]
 */
class MongoQueryNodeOr(val members: List[MongoQueryNode]) extends MongoQueryNode {

    override def isOr: Boolean = true

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[MongoQueryNodeOr] && this.members == q.asInstanceOf[MongoQueryNodeOr].members
    }

    override def toQueryStringNotFirst() = {
        var str = "$or: ["

        var first = true
        members.foreach(m => {
            if (first)
                first = false
            else
                str += ", "
            str += "{" + m + "}"
        })
        str += "]"
        str
    }

    /**
     * Flatten several nested ORs into a single one
     */
    def flattenOrs: MongoQueryNodeOr = {
        val optMembers = new Queue[MongoQueryNode]
        for (m <- members) {
            if (m.isOr)
                optMembers ++= m.asInstanceOf[MongoQueryNodeOr].members
            else
                optMembers += m
        }

        val optList = optMembers.toList
        if (optList != members) {
            if (logger.isTraceEnabled())
                logger.trace("Optimized [" + members + "] into [" + optList + "]")
            new MongoQueryNodeOr(optList)
        } else this
    }

    /**
     * Group WHEREs:
     * 	OR(..., WHERE(W1), WHERE(W2)) => OR(..., WHERE(W1 || W2)
     */
    def groupWheres: MongoQueryNodeOr = {
        // Split WHERE nodes and other nodes
        val wheres = new Queue[MongoQueryNode]
        val others = new Queue[MongoQueryNode]
        for (m <- members) {
            if (m.isWhere)
                wheres += m
            else
                others += m
        }

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
            if (logger.isTraceEnabled())
                logger.trace("Optimized [" + members + "] into [" + optMembers + "]")
                
            new MongoQueryNodeOr(optMembers)
        }
    }
}
