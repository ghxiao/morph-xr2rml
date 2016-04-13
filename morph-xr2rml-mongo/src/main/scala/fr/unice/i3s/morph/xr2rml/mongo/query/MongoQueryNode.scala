package fr.unice.i3s.morph.xr2rml.mongo.query

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.exception.MorphException

/**
 * Abstract representation of a MongoDB query. This is used during query construction and
 * optimization phases, and allows to generate concrete MongoDB queries.
 * 
 * @author Franck Michel, I3S laboratory
 */
abstract class MongoQueryNode {

    val logger = Logger.getLogger(this.getClass().getName());

    var projection: List[MongoQueryProjection] = List.empty

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

    /** Is the current abstract node a FIELD node? */
    def isElemMatch: Boolean = false

    /**
     * Build the final concrete query string corresponding to that abstract query object.
     * This does not apply to a UNION query, in which case an exception is thrown.
     *
     * An AND is returned as a single result with AND members separated by a comma, because MongoDB
     * implicitly makes an AND between the fields of the top-level query document.
     *
     * @param fromPart the query string that comes from the logical source of the triples map,
     * to be appended to the query we will produce from this MongoQueryNode instance .
     * It must not be in '{' and '}'.
     * @return query part of the MongoDB collection.find() method
     */
    def toTopLevelQuery(fromPart: String): String = {
        val from =
            if (fromPart.isEmpty) ""
            else fromPart + ", "

        if (this.isAnd) // replace the top-level AND by its members
            "{" + from + this.asInstanceOf[MongoQueryNodeAnd].queryMembersToString + "}"
        else if (this.isInstanceOf[MongoQueryNodeNotSupported])
            "{" + from + "}"
        else if (this.isUnion)
            throw new MorphException("Unsupported call: toTopLevelQuery cannot apply to a UNION query")
        else
            "{" + from + this.toString + "}"
    }

    /**
     * Build the concrete query string corresponding to that abstract query object
     * when it is a top-level query object.
     * The result does not contain the encapsulating '{ and '}' of a final query string.
     */
    def toString(): String

    /**
     * Build the concrete projection part of the MongoDB collection.find() method.
     * The result does contains encapsulating '{ and '}'.
     */
    def toTopLevelProjection(): String = {
        if (this.isAnd) // replace the top-level AND by its members 
            "{" + this.asInstanceOf[MongoQueryNodeAnd].members.flatMap(_.projection).mkString(", ") + "}"
        else
            "{" + this.projection.mkString(", ") + "}"
    }

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
            case a: MongoQueryNodeWhere => a

            case a: MongoQueryNodeUnion => {
                new MongoQueryNodeUnion(a.flattenUnions.groupWheres.members.map(m => m.optimizeQuery))
            }

            case a: MongoQueryNodeField => {
                // Remove NOT_SUPPORTED elements and optimize other members
                val optMembers = a.members.filterNot(_.isInstanceOf[MongoQueryNodeNotSupported]).map(m => m.optimizeQuery)

                // Remove the FIELD node if all members are NOT_SUPPORTED
                if (optMembers.isEmpty)
                    new MongoQueryNodeNotSupported("Empty FIELD after removing a NOT_SUPPORTED")
                else
                    new MongoQueryNodeField(a.path, optMembers, a.projection)
            }

            case a: MongoQueryNodeElemMatch => {
                // Merge nested ANDs with the ELEMMATCH
                var optMembers = a.flattenAnds.members

                // Remove NOT_SUPPORTED elements and optimize other members
                optMembers = optMembers.filterNot(_.isInstanceOf[MongoQueryNodeNotSupported]).map(m => m.optimizeQuery)
                if (optMembers.isEmpty)
                    new MongoQueryNodeNotSupported("Empty ELEMMATCH after removing a NOT_SUPPORTED")
                else new MongoQueryNodeElemMatch(optMembers)
            }

            case a: MongoQueryNodeAnd => {
                // Flatten nested ANDs and group WHEREs
                var optMembers = a.flattenAnds.groupWheres.members

                // Remove NOT_SUPPORTED elements and optimize other members
                optMembers = optMembers.filterNot(_.isInstanceOf[MongoQueryNodeNotSupported]).map(m => m.optimizeQuery)

                if (optMembers.isEmpty)
                    new MongoQueryNodeNotSupported("Empty AND")
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

            // Anything else
            case a => a
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

            // Anything else
            case a => a
        }

        if (this != optd) {
            if (logger.isTraceEnabled())
                logger.trace("Optimized " + this + " into " + optd)
            optd
        } else this
    }
}

object MongoQueryNode {

    /**
     * Translates a JSONPath or JavaScript path like '.p[10].q["r"]' into a MongoDB path 'p.10.q.r'
     */
    def dotNotation(path: String): String = {
        // .p[10].q[6]["r"] -> .p[10].q[6][r] -> .p.10..q.6..r. -> .p.10.q.6.r.
        var str: String = path.replace("\"", "").replace("[", ".").replace("]", ".").replace("..", ".")

        // Remove heading dot
        str = if (str.startsWith(".")) str.substring(1) else str

        // Remove tailing dot
        str = if (str.endsWith(".") && str.size >= 2) str.substring(0, str.size - 1) else str
        str
    }

    /**
     * Makes the fusion of a set of MongoQueryNode: when several queries of type FIELD have the same path,
     * they are fusioned into a single FIELD query.
     */
    def fusionQueries(qs: List[MongoQueryNode]): List[MongoQueryNode] = {
        if (qs.isEmpty || qs.length == 1)
            return qs
        else {
            // Select all nodes that are not FIELDs
            val qsNonFields = qs.filterNot(_.isField)

            // Select all FIELD nodes that have the same path as the first FIELD node
            val qsFields = qs.filter(_.isField).map(_.asInstanceOf[MongoQueryNodeField])
            val fieldHead = qsFields.head
            val fieldsWithSamePath = qsFields.tail.filter(f => f.path == fieldHead.path)

            // Select all FIELD nodes that have a different path from the first FIELD node
            val fieldsWithOtherPath = qsFields.tail.filterNot(f => f.path == fieldHead.path)

            // Result: 
            qsNonFields ++ // all non-field elements
                (fusionQueries(fieldsWithOtherPath) // all other FIELD nodes fusioned when possible
                    :+ fusionFields(fieldHead +: fieldsWithSamePath)) // fusion of all FIELD nodes that have the same path as the first FIELD
        }
    }

    /**
     * Merge several queries of type FIELD that have the same path.
     * Examples:
     * 	q1: 'a': {$gt:10}
     * 	q2: 'a': {$lt:20}
     * 		=> 'a': {$gt:10, $lt:20}
     *
     * 	q1: 'a.b': {$size:10, $elemMatch:{A}}
     * 	q2: 'a.b': {$elemMatch:{B}}
     * 		=> 'a.b': {$size:10, $elemMatch:{A, B}}
     *
     *  If all FIELD nodes do not have the path then an exception is thrown.
     */
    private def fusionFields(q: List[MongoQueryNodeField]): MongoQueryNodeField = {
        var q1 = q.head
        for (q2 <- q.tail)
            q1 = fusionTwoFields(q1, q2)
        q1
    }

    /**
     * Merge 2 queries of type FIELD that have the same path.
     * Examples:
     * 	q1: 'a': {$gt:10}
     * 	q2: 'a': {$lt:20}
     * 		=> 'a': {$gt:10, $lt:20}
     *
     * 	q1: 'a.b': {$size:10, $elemMatch:{A}}
     * 	q2: 'a.b': {$elemMatch:{B}}
     * 		=> 'a.b': {$size:10, $elemMatch:{A, B}}
     *
     *  If the 2 FIELD nodes do not have the same path then an exception is thrown.
     */
    private def fusionTwoFields(q1: MongoQueryNodeField, q2: MongoQueryNodeField): MongoQueryNodeField = {
        if (q1.path != q2.path)
            throw new MorphException("Error: both queries should have the same field name. Mistaken call.")

        // Group all ELEMMATCH members into a single ELEMMATCH
        val all_EM_members = (q1.members.filter(_.isElemMatch) ++ q2.members.filter(_.isElemMatch))
            .flatMap(_.asInstanceOf[MongoQueryNodeElemMatch].members)

        // Group all non-ELEMMATCH members of q1 and q2 in a single list
        val q1_NEM = q1.members.filterNot(_.isElemMatch) ++ q2.members.filterNot(_.isElemMatch)

        // Build a new FIELD that will replace q1 and q2, 
        // in which the ELEMMATCH nodes of q1 and q2 are grouped in a single one
        if (!all_EM_members.isEmpty)
            new MongoQueryNodeField(q1.path, q1_NEM :+ new MongoQueryNodeElemMatch(all_EM_members), q1.projection ++ q2.projection)
        else
            new MongoQueryNodeField(q1.path, q1_NEM, q1.projection ++ q2.projection)
    }
}