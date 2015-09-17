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
                new MongoQueryNodeOr(optMembers)
            } else this
        }
    }

    /**
     * Check if there is at least one WHERE member in the OR
     */
    def hasWhere: Boolean = {
        !members.filter(_.isWhere).isEmpty
    }

    /**
     * Check if there is at least one UNION member in the OR
     */
    def hasUnion: Boolean = {
        !members.filter(_.isUnion).isEmpty
    }

    /**
     * If the OR contains one WHERE, this method returns a pair containing the WHERE node as first term
     * and the list of other nodes as second term.
     * If there is no WHERE node in the members then return None
     */
    def splitWhereAndOthers: Option[(MongoQueryNodeWhere, List[MongoQueryNode])] = {

        // Merge multiple WHEREs if any
        val optimized = this.groupWheres

        // Split WHERE nodes and other nodes
        val (where, others) = optimized.members.partition(_.isWhere)
        if (where.isEmpty)
            None
        else
            Some((where(0).asInstanceOf[MongoQueryNodeWhere], others))
    }

    /**
     * Rule w1: OR(A1,...An, W) <=> UNION(OR(A1,...An), W)
     * 
     * If this OR node has no WHERE, then it is returned. Otherwise a UNION is returned.
     * 
     */
    def rewriteOrWhere: MongoQueryNode = {
        if (this.hasWhere) {
            val (where, others) = this.splitWhereAndOthers.get
            new MongoQueryNodeUnion(new MongoQueryNodeOr(others), where)
        } else this
    }

    /**
     * Rule w2: OR(A1,...An, AND(B1,...Bn, W)) <=> UNION(OR(A1,...An), AND(B1,...Bn, W)).
     *
     * If this OR node has no AND with a WHERE, then it is returned. Otherwise a UNION is returned.
     */
    def rewriteOrAndWhere: MongoQueryNode = {
        val (ands, others) = members.partition(_.isAnd)

        if (!ands.isEmpty) {
            // Split the ANDs with a WHERE and the ANDs without
            val (andsWithWhere, andsWithoutWhere) = ands.map(a => a.asInstanceOf[MongoQueryNodeAnd]).partition(_.hasWhere)
            if (!andsWithWhere.isEmpty) {
                // Select the first AND with a WHERE, and split it into the WHERE and other nodes
                val (w, b1_bn) = andsWithWhere.head.splitWhereAndOthers.get

                // A1,...An = all other nodes: non-AND nodes, AND nodes with no WHERE, remaining AND nodes with a WHERE
                val a1_an = others ++ andsWithoutWhere ++ andsWithWhere.tail

                new MongoQueryNodeUnion(new MongoQueryNodeOr(a1_an), new MongoQueryNodeAnd(b1_bn :+ w))
            } else this
        } else this
    }

    /**
     * Rule w6: OR(A1,...An, UNION(B1,...Bn)) <=> UNION(OR(A1,...An), B1,...Bn)).
     *
     * If this OR node has no UNION then it is returned. Otherwise a UNION is returned.
     */
    def rewriteOrUnion: MongoQueryNode = {
        val (unions, others) = members.partition(_.isUnion)

        if (!unions.isEmpty) {
            // B1,...Bn: members of the first UNION node
            val b1_bn = unions.head.asInstanceOf[MongoQueryNodeUnion].members

            // A1,...An = all other nodes: non-UNION nodes, remaining UNION nodes
            val a1_an = others ++ unions.tail

            new MongoQueryNodeUnion(new MongoQueryNodeOr(a1_an) +: b1_bn)
        } else this
    }
}
