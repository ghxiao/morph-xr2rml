package fr.unice.i3s.morph.xr2rml.mongo.abstractquery

import es.upm.fi.dia.oeg.morph.base.MorphBaseResultRdfTerms
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.query.AbstractQuery
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryOptimizer
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryProjection
import es.upm.fi.dia.oeg.morph.base.querytranslator.TpBindings

/**
 * Representation of the UNION abstract query of several abstract queries
 *
 * @param lstMembers the abstract query members of the union, flattened if there are embedded unions
 * @param limit the value of the optional LIMIT keyword in the SPARQL graph pattern
 *
 * @author Franck Michel, I3S laboratory
 */
class AbstractQueryUnion(
    lstMembers: List[AbstractQuery],
    lim: Option[Long])
        extends AbstractQuery(new TpBindings, lim) {

    // Construction-time optimization: flatten (merge) nested unions into a single one
    val members: List[AbstractQuery] = lstMembers.flatMap { m =>
        if (m.isInstanceOf[AbstractQueryUnion])
            m.asInstanceOf[AbstractQueryUnion].members
        else List(m)
    }

    val logger = Logger.getLogger(this.getClass().getName());

    override def equals(a: Any): Boolean = {
        a.isInstanceOf[AbstractQueryUnion] &&
            this.members == a.asInstanceOf[AbstractQueryUnion].members
    }

    override def toString = {
        "[" + members.mkString("\n] UNION [\n") + "\n]" + limitStr
    }

    override def toStringConcrete: String = {
        "[" + members.map(q => q.toStringConcrete).mkString("\n] UNION [\n") + "\n]" + limitStr
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
        if (!members.isEmpty)
            res = members.head.isTargetQuerySet
        for (q <- members)
            res = res && q.isTargetQuerySet
        res
    }

    /**
     * Return the list of SPARQL variables projected in all the abstract queries of this UNION query
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
     * Optimize the members of the union
     */
    override def optimizeQuery(optimizer: MorphBaseQueryOptimizer): AbstractQuery = {

        if (!optimizer.selfUnionElimination) return this

        if (members.size == 1) { // security test but abnormal case, should never happen
            logger.warn("Unexpected case: union with only one member: " + this.toString)
            return members.head
        }

        if (logger.isDebugEnabled)
            logger.debug("\n------------------ Optimizing query ------------------\n" + this)

        // First, optimize all members individually
        var membersV = members.map(_.optimizeQuery(optimizer))
        if (logger.isDebugEnabled) {
            val res = if (membersV.size == 1) membersV.head.setLimit(limit) else new AbstractQueryUnion(membersV, limit)
            if (this != res)
                logger.debug("\n------------------ Members optimized individually giving new query ------------------\n" + res)
        }

        var continue = true
        while (continue) {

            for (i: Int <- 0 to (membersV.size - 2) if continue) { // from first until second to last (avant-dernier)
                for (j: Int <- (i + 1) to (membersV.size - 1) if continue) { // from i+1 until last

                    val left = membersV(i)
                    val right = membersV(j)

                    // Union of 2 atomic queries
                    if (left.isInstanceOf[AbstractQueryAtomicMongo] && right.isInstanceOf[AbstractQueryAtomicMongo]) {
                        val opt = left.asInstanceOf[AbstractQueryAtomicMongo].mergeForUnion(right)
                        if (opt.isDefined) {
                            //     i     j     =>   slice(0,i),  merged(i,j),  slice(i+1,j),  slice(j+1, size)
                            // (0, 1, 2, 3, 4) =>   0         ,  merged(1,3),  2           ,  4
                            membersV = membersV.slice(0, i) ++ List(opt.get) ++ membersV.slice(i + 1, j) ++ membersV.slice(j + 1, membersV.size)
                            continue = false
                            if (logger.isDebugEnabled) logger.debug("Self-union eliminated between queries " + i + " and " + j)
                        } else if (logger.isDebugEnabled)
                            logger.debug("No self-union elimination between queries " + i + " and " + j)
                    }
                } // end for j
            } // end for i

            if (continue) {
                // There was no change in the last run, we cannot do anymore optimization
                if (membersV.size == 1)
                    return membersV.head.setLimit(limit)
                else
                    return new AbstractQueryUnion(membersV, limit)
            } else {
                // There was a change in this run, let's rerun the optimization with the new list of members
                continue = true
                if (logger.isDebugEnabled) {
                    val res = if (membersV.size == 1) membersV.head.setLimit(limit) else new AbstractQueryUnion(membersV, limit)
                    logger.debug("\n------------------ Query optimized into ------------------\n" + res)
                }
            }
        }

        throw new MorphException("We should not quit the function this way.")
    }

    /**
     * Execute each query of the union, generate the RDF terms for each of the result documents,
     * then make a UNION of all the results
     *
     * @param dataSourceReader the data source reader to query the database
     * @param dataTrans the data translator to create RDF terms
     * @return a list of MorphBaseResultRdfTerms instances, one for each result document of each member
     * of the union query. May return an empty result but NOT null.
     */
    override def generateRdfTerms(
        dataSourceReader: MorphBaseDataSourceReader,
        dataTranslator: MorphBaseDataTranslator): List[MorphBaseResultRdfTerms] = {

        val start = System.currentTimeMillis
        if (logger.isInfoEnabled) {
            logger.info("===============================================================================");
            logger.info("Generating RDF triples from union query:\n" + this.toStringConcrete);
        }

        var res = List[MorphBaseResultRdfTerms]()

        for (m <- members if (!limit.isDefined || (limit.isDefined && res.size < limit.get))) {

            // If the member is not an atomic query then there are triples of different types in the result. So we cannot just
            // take a subset of 'limit' triples of them, as e.g. if this is a join, a join on the subset of triples may no longer 
            // return anything. The limit is taken care of when processing that sub-query.
            // Conversely, if it is an atomic query, then we can limit the number of triples but only if it is bound to only
            // one triples map. Otherwise, we are back to the previous case: several types of triples, so we cannot take a subset.
            val considerLimit = m.isInstanceOf[AbstractQueryAtomicMongo] && m.tpBindings.size == 1
            
            val resultsM = m.generateRdfTerms(dataSourceReader, dataTranslator)
            if (considerLimit && limit.isDefined) {
                if (res.size < limit.get) {
                    val lim = limit.get - res.size
                    res = res ++ resultsM.take(lim.toInt)
                }
            } else
                res = res ++ resultsM
        }

        logger.info("Union computed " + res.size + " triples, in " + (System.currentTimeMillis - start) + " ms.")
        res
    }
}