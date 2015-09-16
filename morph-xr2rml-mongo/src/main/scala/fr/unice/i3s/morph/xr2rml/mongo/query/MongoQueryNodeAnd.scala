package fr.unice.i3s.morph.xr2rml.mongo.query

import scala.collection.mutable.Queue

/**
 * MongoDB query starting with an AND node:
 * 	$and: [{doc1}, {doc1}... {docN}]
 */
class MongoQueryNodeAnd(val members: List[MongoQueryNode]) extends MongoQueryNode {

    override def isAnd: Boolean = true

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[MongoQueryNodeAnd] && this.members == q.asInstanceOf[MongoQueryNodeAnd].members
    }

    override def toQueryStringNotFirst() = {
        var str = "$and: ["

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
     * Flatten several nested ANDs into a single one
     */
    def flattenAnds: MongoQueryNodeAnd = {
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
            new MongoQueryNodeAnd(optList)
        } else this
    }

    /**
     * Group WHEREs:
     * 	AND(..., WHERE(W1), WHERE(W2)) => AND(..., WHERE(W1 && W2)
     */
    def groupWheres: MongoQueryNodeAnd = {
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
                        groupedWhereConds += " && (" + whereCond + ")"
                }
                // Create a new list of nodes where the multiple WHERE nodes are replaced by a single one 
                (others :+ new MongoQueryNodeWhere(groupedWhereConds)).toList
            }
            if (logger.isTraceEnabled())
                logger.trace("Optimized [" + members + "] into [" + optMembers + "]")

            new MongoQueryNodeAnd(optMembers)
        }
    }
}