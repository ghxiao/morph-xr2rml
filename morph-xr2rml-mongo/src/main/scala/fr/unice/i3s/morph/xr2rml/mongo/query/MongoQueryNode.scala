package fr.unice.i3s.morph.xr2rml.mongo.query

import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import scala.collection.mutable.Queue
import org.apache.log4j.Logger

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

    /** Is the current abstract node a WHERE node? */
    def isWhere: Boolean = false

    /** Build the concrete query string corresponding to that abstract query object */
    override def toString(): String = { toQueryStringNotFirst() }

    /**
     * Build the concrete query string corresponding to that abstract query object
     *  if this one is the top level query object (the root)
     */
    def toQueryStringNotFirst(): String

    /**
     * Apply optimizations to the query repeatedly, until the optimization no longer changes anything.
     */
    def optimize: MongoQueryNode = {
        var q = this
        var qopt = q.optimizeQuery

        // The query optimization changes things than can allow for new optimizations. Therefore 
        // we call the optimize method until there is no more changes
        while (q != qopt) {
            q = qopt
            qopt = qopt.optimizeQuery
        }
        q
    }

    /**
     * Apply different forms of optimization to the query:
     * <ul>
     * 	<li>flatten nested ANDs</li>
     * 	<li>flatten nested ORs</li>
     * 	<li>in ORs and ANDs, groups several WHERE conditions into a single one</li>
     * </ul>
     */
    private def optimizeQuery: MongoQueryNode = {
        this match {
            case a: MongoQueryNodeCond => a

            case a: MongoQueryNodeCompare => a

            case a: MongoQueryNodeExists => a

            case a: MongoQueryNodeNotExists => a

            case a: MongoQueryNodeNop => a

            case a: MongoQueryNodeWhere => a

            case a: MongoQueryNodeField => new MongoQueryNodeField(a.field, a.next.optimizeQuery)

            case a: MongoQueryNodeElemMatch => new MongoQueryNodeElemMatch(a.next.optimizeQuery)

            case a: MongoQueryNodeAnd =>
                val optMembers = a.flattenAnds.groupWheres.members
                new MongoQueryNodeAnd(optMembers.map(m => m.optimizeQuery))

            case a: MongoQueryNodeOr =>
                val optMembers = a.flattenOrs.groupWheres.members
                new MongoQueryNodeOr(optMembers.map(m => m.optimizeQuery))
        }
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