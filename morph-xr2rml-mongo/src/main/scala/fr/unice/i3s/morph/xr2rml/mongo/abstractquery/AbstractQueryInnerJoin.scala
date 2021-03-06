package fr.unice.i3s.morph.xr2rml.mongo.abstractquery

import org.apache.log4j.Logger

import es.upm.fi.dia.oeg.morph.base.MorphBaseResultRdfTerms
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.AbstractQuery
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryProjection
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryOptimizer
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.TpBindings

/**
 * Representation of the INNER JOIN abstract query generated from the join of several basic graph patterns.
 * It is not allowed to create an inner join instance with nested inner joins.
 *
 * @param lstMembers the abstract query members of the inner join, flattened if there are embedded inner joins
 * @param limit the value of the optional LIMIT keyword in the SPARQL graph pattern
 *
 * @author Franck Michel, I3S laboratory
 */
class AbstractQueryInnerJoin(
    lstMembers: List[AbstractQuery],
    lim: Option[Long])
        extends AbstractQuery(new TpBindings, lim) {

    val logger = Logger.getLogger(this.getClass().getName())

    // Construction-time optimization: flatten (merge) nested joins into a single one
    val members: List[AbstractQuery] = lstMembers.flatMap { m =>
        if (m.isInstanceOf[AbstractQueryInnerJoin])
            m.asInstanceOf[AbstractQueryInnerJoin].members
        else List(m)
    }

    if (members.size < 2)
        throw new MorphException("Attempt to create an inner join with less than 2 members: " + members)

    override def equals(a: Any): Boolean = {
        a.isInstanceOf[AbstractQueryInnerJoin] &&
            this.members == a.asInstanceOf[AbstractQueryInnerJoin].members
    }

    /**
     * String representation as if joined queries were embedded in each other.
     * For joined queries q1, q2, q3, this shall display: <br><code>
     * q1 INNER JOIN [
     *   q2 INNER JOIN [
     *     q3 INNER JOIN q4 on set
     *   ] ON set
     * ] ON set LIMIT 10
     * </code>
     */
    override def toString = {
        val subq =
            if (members.size >= 3) new AbstractQueryInnerJoin(members.tail, None)
            else members.tail.head

        "[" + members.head.toString + "\n" +
            "] INNER JOIN [\n" +
            subq.toString + "\n" +
            "] ON " + AbstractQuery.getSharedVariables(members.head, subq) + limitStr
    }

    override def toStringConcrete: String = {
        val subq =
            if (members.size >= 3) new AbstractQueryInnerJoin(members.tail, None)
            else members.tail.head

        "[" + members.head.toStringConcrete + "\n" +
            "] INNER JOIN [\n" +
            subq.toStringConcrete + "\n" +
            "] ON " + AbstractQuery.getSharedVariables(members.head, subq) + limitStr
    }

    /**
     * Translate all atomic abstract queries of this abstract query into concrete queries.
     * @param translator the query translator
     */
    override def translateAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit = {
        for (q <- members)
            q.translateAbstactQueriesToConcrete(translator)
    }

    /**
     * Check if atomic abstract queries within this query have a target query properly initialized
     * i.e. targetQuery is not empty
     */
    override def isTargetQuerySet: Boolean = {
        var res: Boolean = false
        if (members.nonEmpty)
            res = members.head.isTargetQuerySet
        for (q <- members)
            res = res && q.isTargetQuerySet
        res
    }

    /**
     * Return the list of SPARQL variables projected in this abstract query
     */
    override def getVariables: Set[String] = {
        members.flatMap(m => m.getVariables).toSet
    }

    /**
     * Get the xR2RML projection of variable ?x in this query.
     *
     * @param varName the variable name
     * @return a set of projections in which the 'as' field is defined and equals 'varName'
     */
    override def getProjectionsForVariable(varName: String): Set[AbstractQueryProjection] = {
        members.flatMap(m => m.getProjectionsForVariable(varName)).toSet
    }

    /**
     * Try to merge atomic queries among the members of the inner join
     *
     * @note in this function we use the slice method:
     * list.slice(start, end) : from start (included) until end (excluded), i.e. slice(n,n) returns an empty list
     */
    override def optimizeQuery(optimizer: MorphBaseQueryOptimizer): AbstractQuery = {

        if (members.size == 1) { // security test but abnormal case, should never happen
            logger.warn("Unexpected case: inner join with only one member: " + this.toString)
            return members.head.setLimit(limit)
        }

        if (logger.isDebugEnabled)
            logger.debug("\n------------------ Optimizing query ------------------\n" + this)

        // First, optimize all members individually
        var membersV = members.map(_.optimizeQuery(optimizer))
        if (logger.isDebugEnabled) {
            val res = if (membersV.size == 1) membersV.head.setLimit(limit) else new AbstractQueryInnerJoin(membersV, limit)
            if (this != res)
                logger.debug("\n------------------ Members optimized individually giving new query ------------------\n" + res)
        }

        var continue = true
        while (continue) {

            for (i: Int <- 0 to (membersV.size - 2) if continue) { // from first until second to last (avant-dernier)
                for (j: Int <- (i + 1) to (membersV.size - 1) if continue) { // from i+1 until last

                    val left = membersV(i)
                    val right = membersV(j)

                    // Inner-join of 2 Atomic Queries
                    if (left.isInstanceOf[AbstractQueryAtomicMongo] && right.isInstanceOf[AbstractQueryAtomicMongo]) {

                        var leftAtom = left.asInstanceOf[AbstractQueryAtomicMongo]
                        var rightAtom = right.asInstanceOf[AbstractQueryAtomicMongo]

                        // ----- Try to narrow down joined atomic queries by propagating conditions from one to the other -----
                        if (optimizer.propagateConditionFromJoin) {
                            leftAtom = leftAtom.propagateConditionFromJoinedQuery(rightAtom)
                            if (logger.isDebugEnabled && leftAtom != left)
                                logger.debug("Propagated condition of query " + j + " into query " + i)

                            rightAtom = rightAtom.propagateConditionFromJoinedQuery(leftAtom)
                            if (logger.isDebugEnabled && rightAtom != right)
                                logger.debug("Propagated condition of query " + i + " into query " + j)

                            if (leftAtom != left || rightAtom != right) {
                                membersV = membersV.slice(0, i) ++ List(leftAtom) ++ membersV.slice(i + 1, j) ++ List(rightAtom) ++ membersV.slice(j + 1, membersV.size)
                                continue = false
                            } else if (logger.isDebugEnabled)
                                logger.debug("No condition propagated between queries " + i + " and " + j)
                        }

                        // ----- Try to eliminate a Self-Join by merging the 2 atomic queries -----
                        if (optimizer.selfJoinElimination) {
                            val merged = leftAtom.mergeForInnerJoin(rightAtom)
                            if (merged.isDefined) {
                                //     i     j     =>   slice(0,i),  merged(i,j),  slice(i+1,j),  slice(j+1, size)
                                // (0, 1, 2, 3, 4) =>   0         ,  merged(1,3),  2           ,  4
                                membersV = membersV.slice(0, i) ++ List(merged.get) ++ membersV.slice(i + 1, j) ++ membersV.slice(j + 1, membersV.size)
                                continue = false
                                if (logger.isDebugEnabled) logger.debug("Self-join eliminated between queries " + i + " and " + j)
                            } else if (logger.isDebugEnabled)
                                logger.debug("No self-join elimination between queries " + i + " and " + j)
                        }
                    }
                } // end for j
            } // end for i

            if (continue) {
                // There was no change in the last run, we cannot do anymore optimization
                if (membersV.size == 1)
                    // If we merged 2 atomic queries we may only have one remaining query
                    return membersV.head.setLimit(limit)
                else
                    return new AbstractQueryInnerJoin(membersV, limit)
            } else {
                // There was a change in this run, let's rerun the optimization with the new list of members
                continue = true
                if (logger.isDebugEnabled) {
                    val res = if (membersV.size == 1) membersV.head.setLimit(limit) else new AbstractQueryInnerJoin(membersV, limit)
                    logger.debug("\n------------------ Query optimized into ------------------\n" + res)
                }
            }
        }
        throw new MorphException("We should not quit the function this way.")
    }

    /**
     * Execute the left and right queries, generate the RDF terms for each of the result documents,
     * then make a JOIN of all the results
     *
     * @param dataSourceReader the data source reader to query the database
     * @param dataTrans the data translator to create RDF terms
     * @return a list of MorphBaseResultRdfTerms instances, one for each result document
     * May return an empty result but NOT null.
     * @throws es.upm.fi.dia.oeg.morph.base.exception.MorphException if the triples map bound to the query has no referencing object map
     */
    override def generateRdfTerms(
        dataSourceReader: MorphBaseDataSourceReader,
        dataTranslator: MorphBaseDataTranslator): List[MorphBaseResultRdfTerms] = {

        if (logger.isInfoEnabled) {
            logger.info("===============================================================================");
            logger.info("Generating RDF triples from the inner join query:\n" + this.toStringConcrete)
        }

        var joinResult = Map[Int, MorphBaseResultRdfTerms]()

        // First, generate the triples for both left and right graph patterns of the join
        val leftQ = members.head
        val rightQ =
            if (members.size >= 3) new AbstractQueryInnerJoin(members.tail, None)
            else members.tail.head
        val leftTriples = leftQ.generateRdfTerms(dataSourceReader, dataTranslator)
        val rightTriples = rightQ.generateRdfTerms(dataSourceReader, dataTranslator)

        // The limit really works only when we have 2 atomic queries. Otherwise, we cannot limit the triples returned because
        // we don't know if it will impact the generated triples or not.
        if (limit.isDefined)
            if (!(leftQ.isInstanceOf[AbstractQueryAtomicMongo] && rightQ.isInstanceOf[AbstractQueryAtomicMongo]))
                limit = None

        val start = System.currentTimeMillis
        if (logger.isDebugEnabled)
            logger.debug("Inner joining " + leftTriples.size + " left triples with " + rightTriples.size + " right triples.")
        if (logger.isTraceEnabled) {
            logger.trace("Left triples:\n" + leftTriples.mkString("\n"))
            logger.trace("Right triples:\n" + rightTriples.mkString("\n"))
        }

        val sharedVars = AbstractQuery.getSharedVariables(leftQ, rightQ)
        if (sharedVars.isEmpty) {
            val res = leftTriples ++ rightTriples
            logger.info("Inner join on empty set of variables computed " + res.size + " triples.")
            res
        } else {
            logger.info("Inner join on variable(s) " + sharedVars)
            var nonJoinedLeft: List[MorphBaseResultRdfTerms] = List.empty
            var nonJoinedRight: List[MorphBaseResultRdfTerms] = List.empty

            // For each variable x shared by both graph patterns, select the left and right triples
            // in which at least one term is bound to x, then join the triples on these terms.
            for (x <- sharedVars if (!limit.isDefined || (limit.isDefined && joinResult.size < limit.get))) {

                val leftTripleX = leftTriples.filter(_.hasVariable(x))
                val rightTripleX = rightTriples.filter(_.hasVariable(x))

                for (leftTriple <- leftTripleX if (!limit.isDefined || (limit.isDefined && joinResult.size < limit.get))) {
                    for (rightTriple <- rightTripleX if (!limit.isDefined || (limit.isDefined && joinResult.size < limit.get))) {
                        val leftTerm = leftTriple.getTermForVariable(x)
                        val rightTerm = rightTriple.getTermForVariable(x)
                        if (leftTerm.get == rightTerm.get) {
                            // If there is a match, keep the two triples 
                            if (!joinResult.contains(leftTriple.id))
                                joinResult += (leftTriple.id -> leftTriple)
                            if (!joinResult.contains(rightTriple.id))
                                joinResult += (rightTriple.id -> rightTriple)
                        }
                    }
                }

                // All left and right triples that do not contain any of the shared variables are kept separately
                nonJoinedLeft ++= leftTriples.filter(!_.hasVariable(x))
                nonJoinedRight ++= rightTriples.filter(!_.hasVariable(x))
            }

            val res = joinResult.values.toList
            val resNonJoined = nonJoinedLeft ++ nonJoinedRight
            val result = res ++ resNonJoined
            logger.info("Inner join computed " + res.size + " triples + " + resNonJoined.size + " triples with no shared variable, in " + (System.currentTimeMillis - start) + " ms.")
            if (logger.isTraceEnabled) logger.trace("Join result:\n" + result.mkString("\n"))
            result
        }
    }
}