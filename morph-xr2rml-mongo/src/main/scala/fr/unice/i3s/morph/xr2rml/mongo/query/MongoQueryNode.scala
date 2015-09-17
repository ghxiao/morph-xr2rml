package fr.unice.i3s.morph.xr2rml.mongo.query

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.exception.MorphException

/**
 * Abstract class to describe MongoDB queries
 */
abstract class MongoQueryNode {

    val logger = Logger.getLogger(this.getClass().getName());

    /** Is the current abstract node a FIELD node? */
    def isField: Boolean = false

    /** Is the current abstract node an AND node? */
    def isAnd: Boolean = false

    /** Is the current abstract node an OR node? */
    def isOr: Boolean = false

    /** Is the current abstract node an UNION node? */
    def isUnion: Boolean = false

    /** Is the current abstract node a WHERE node? */
    def isWhere: Boolean = false

    /**
     * Build the concrete query string corresponding to that abstract query object
     * when it is the top-level query object (the root)
     */
    override def toString(): String = { toQueryStringNotFirst() }

    /**
     * Build the concrete query string corresponding to that abstract query object
     * when it is not the top-level query object
     */
    def toQueryStringNotFirst(): String

    /**
     * Apply optimizations to the query repeatedly, and try to pull up WHERE nodes,
     * until the optimization no longer changes anything.
     */
    def optimize: MongoQueryNode = {
        var q = this
        var qopt: MongoQueryNode = null
        var continue = true

        while (continue) {

            // Each time the query optimization changes something, it can open the opportunity for new optimizations. 
            // Therefore we call the optimize method until there is no more changes
            qopt = q.optimizeQuery
            while (q != qopt) {
                q = qopt
                qopt = q.optimizeQuery
            }

            // Pull up the WHEREs by using UNIONs
            qopt = q.pullUpWheres

            if (q == qopt)
                continue = false
            else
                q = qopt
        }
        q
    }

    /**
     * Apply different forms of optimization to the query:
     * <ul>
     * 	<li>flatten nested ANDs</li>
     * 	<li>flatten nested ORs</li>
     *  <li>merge ELEMMATCH with nested AND elements</li>
     * 	<li>in ORs and ANDs, groups several WHERE conditions into a single one</li>
     *  <li>Replace AND of 1 term by the term itself, and OR of 1 term by the term itself</li>
     *  <li>Remove NOT_SUPPORTED nodes</li>
     *  <li>Replace empty ANDs, empty ORs and empty ELEMMATCHs with NOT_SUPPORTED</li>
     * </ul>
     */
    def optimizeQuery: MongoQueryNode = {
        val optd = this match {
            case a: MongoQueryNodeCond => a
            case a: MongoQueryNodeCompare => a
            case a: MongoQueryNodeExists => a
            case a: MongoQueryNodeNotExists => a
            case a: MongoQueryNodeNotSupported => a
            case a: MongoQueryNodeWhere => a

            case a: MongoQueryNodeUnion => {
                new MongoQueryNodeUnion(a.flattenUnions.groupWheres.members.map(m => m.optimizeQuery))
            }

            case a: MongoQueryNodeField => {
                // Remove the FIELD node if the next is a NOT_SUPPORTED element
                if (a.next.isInstanceOf[MongoQueryNodeNotSupported])
                    new MongoQueryNodeNotSupported("Emptry FIELD")
                else new MongoQueryNodeField(a.field, a.next.optimizeQuery)
            }

            case a: MongoQueryNodeElemMatch => {
                // Merge nested ANDs with the ELEMMATCH
                var optMembers = a.flattenAnds.members

                // Remove NOT_SUPPORTED elements and optimize members
                optMembers = optMembers.filterNot(_.isInstanceOf[MongoQueryNodeNotSupported]).map(m => m.optimizeQuery)
                if (optMembers.isEmpty)
                    new MongoQueryNodeNotSupported("Emptry ELEMMATCH")
                else new MongoQueryNodeElemMatch(optMembers)
            }

            case a: MongoQueryNodeAnd => {
                // Flatten nested ANDs and group WHEREs
                var optMembers = a.flattenAnds.groupWheres.members

                // Remove NOT_SUPPORTED elements and optimize members
                optMembers = optMembers.filterNot(_.isInstanceOf[MongoQueryNodeNotSupported]).map(m => m.optimizeQuery)

                if (optMembers.isEmpty)
                    new MongoQueryNodeNotSupported("Emptry AND")
                else if (optMembers.length == 1)
                    optMembers(0) // Replace AND of 1 term with the term itself
                else new MongoQueryNodeAnd(optMembers)
            }

            case a: MongoQueryNodeOr => {
                // Flatten nested ORs and group WHEREs
                var optMembers = a.flattenOrs.groupWheres.members

                // Remove the OR is there is at least one NOT_SUPPORTED element in it
                val notSupportedMembers = optMembers.filter(_.isInstanceOf[MongoQueryNodeNotSupported])
                if (!notSupportedMembers.isEmpty) {
                    logger.warn("Removing OR node due to NOT_SUPPORTED element: " + a.toString)
                    new MongoQueryNodeNotSupported("OR node with NOT_SUPPORTED element")
                } else {
                    // Optimize members
                    optMembers = optMembers.map(m => m.optimizeQuery)

                    if (optMembers.length == 1)
                        optMembers(0) // Replace OR of 1 term with the term itself
                    else new MongoQueryNodeOr(optMembers)
                }
            }
        }

        if (this != optd) {
            if (logger.isTraceEnabled())
                logger.trace("Optimized " + this + " into " + optd)
            optd
        } else this
    }

    /**
     * Look for WHERE nodes in ANDs and ORs and pull them up by adding UNION nodes instead of ORs,
     * so that WHERE end up in top-level query objects
     */
    def pullUpWheres: MongoQueryNode = {
        val optd = this match {
            case a: MongoQueryNodeCond => a
            case a: MongoQueryNodeCompare => a
            case a: MongoQueryNodeExists => a
            case a: MongoQueryNodeNotExists => a
            case a: MongoQueryNodeNotSupported => throw new MorphException("There should no longer be any NOT_SUPPORTED node at this stage")
            case a: MongoQueryNodeField => a
            case a: MongoQueryNodeElemMatch => a
            case a: MongoQueryNodeWhere => a

            case a: MongoQueryNodeUnion => {
                // Rule w7: UNION(UNION(A1,...An), B1,...Bn) <=> UNION(A1,...An, B1,...Bn)
                val optzd = new MongoQueryNodeUnion(a.members.map(m => m.pullUpWheres))
                if (optzd == a) a else optzd
            }

            case a: MongoQueryNodeAnd => {
                val optzd = new MongoQueryNodeAnd(a.members.map(m => m.pullUpWheres))
                val togo = if (optzd == a) a else optzd

                if (togo.hasUnion) {
                    // Rule w5: AND(A1,...An, UNION(B1,...Bn)) <=> UNION(AND(A1,...An, B1),... AND(A1,...An, Bn))
                    togo.rewriteAndUnion
                } else {
                    // Rule w4: AND(A1,...An, OR(B1,...Bn, W)) <=> UNION(AND(A1,...An, OR(B1,...Bn)), AND(A1,...An, W))
                    togo.rewriteAndOrWhere
                }
            }

            case a: MongoQueryNodeOr => {
                val optzd = new MongoQueryNodeOr(a.members.map(m => m.pullUpWheres))
                val togo = if (optzd == a) a else optzd

                if (togo.hasWhere) {
                    // Rule w1: OR(A1,...An, W) <=> UNION(OR(A1,...An), W)
                    togo.rewriteOrWhere
                } else if (togo.hasUnion) {
                    // Rule w6: OR(A1,...An, UNION(B1,...Bn)) <=> UNION(OR(A1,...An), B1,...Bn)).
                    togo.rewriteOrUnion
                } else {
                    // Rule w2: OR(A1,...An, AND(B1,...Bn, W)) <=> UNION(OR(A1,...An), AND(B1,...Bn, W))
                    togo.rewriteOrAndWhere
                }
            }
        }

        if (this != optd) {
            if (logger.isTraceEnabled())
                logger.trace("Optimized " + this + " into " + optd)
            optd
        } else this
    }
}

object MongoQueryNode {
    object CondType extends Enumeration {
        val Equals, IsNotNull = Value
    }

    /**
     * Returns a MongoDB path consisting of a concatenation of single field names and array indexes in dot notation.
     * It removes the optional heading dot. E.g. dotNotation(.p[5]r) => p.5.r
     */
    def dotNotation(path: String): String = {
        var result =
            if (path.startsWith("."))
                path.substring(1)
            else path
        result = result.replace("[", ".").replace("]", "")
        result
    }
}