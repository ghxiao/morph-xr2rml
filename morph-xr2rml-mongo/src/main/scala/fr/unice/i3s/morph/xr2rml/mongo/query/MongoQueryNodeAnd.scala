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
                        groupedWhereConds += " && (" + whereCond + ")"
                }
                // Create a new list of nodes where the multiple WHERE nodes are replaced by a single one 
                (others :+ new MongoQueryNodeWhere(groupedWhereConds)).toList
            }

            if (optMembers != members) {
                if (logger.isTraceEnabled())
                    logger.trace("Optimized [" + members + "] into [" + optMembers + "]")
                new MongoQueryNodeAnd(optMembers)
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
     * Check if there is at least one UNION member in the AND
     */
    def hasUnion: Boolean = {
        !members.filter(_.isUnion).isEmpty
    }

    /**
     * If the AND contains one WHERE, this method returns a pair containing the WHERE node as first term
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
     * Rule w4: AND(A1,...An, OR(B1,...Bn, W)) <=> UNION(AND(A1,...An, OR(B1,...Bn)), AND(A1,...An, W))
     *
     * If this AND node has no OR with a WHERE, then it is returned. Otherwise a UNION is returned.
     */
    def rewriteAndOrWhere: MongoQueryNode = {

        val (ors, others) = members.partition(_.isOr)

        if (!ors.isEmpty) {
            // Split the ORs with a WHERE and the ORs without
            val (orsWithWhere, orsWithoutWhere) = ors.map(a => a.asInstanceOf[MongoQueryNodeOr]).partition(_.hasWhere)
            if (!orsWithWhere.isEmpty) {
                // Select the first OR with a WHERE, and split it into the WHERE and other nodes
                val (w, b1_bn) = orsWithWhere.head.splitWhereAndOthers.get

                // A1,...An = all other nodes: non-OR nodes, OR nodes with no WHERE, remaining OR nodes with a WHERE
                val a1_an = others ++ orsWithoutWhere ++ orsWithWhere.tail

                new MongoQueryNodeUnion(new MongoQueryNodeAnd(a1_an :+ new MongoQueryNodeOr(b1_bn)), new MongoQueryNodeAnd(b1_bn :+ w))
            } else this
        } else this

        this
    }
    /**
     * Rule w5: AND(A1,...An, UNION(B1,...Bn)) <=> UNION(AND(A1,...An, B1),... AND(A1,...An, Bn))
     *
     * If this AND node has no UNION then it is returned, otherwise a UNION is returned.
     * If it has several UNIONs, the first one is considered as UNION(B1,...Bn).
     */
    def rewriteAndUnion: MongoQueryNode = {

        val (unions, others) = members.partition(_.isUnion)

        if (!unions.isEmpty) {
            // B1,...Bn: members of the first UNION node
            val b1_bn = unions.head.asInstanceOf[MongoQueryNodeUnion].members

            // A1,...An = all other nodes: non-UNION nodes, remaining UNION nodes
            val a1_an = others ++ unions.tail

            val andsOfUnion = b1_bn.map(b => new MongoQueryNodeAnd(a1_an :+ b))
            new MongoQueryNodeUnion(andsOfUnion)
        } else this
    }
}