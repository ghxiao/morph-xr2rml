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

/**
 * Representation of the UNION abstract query of several abstract queries
 *
 * @param boundTriplesMap in the query rewriting context, this is a triples map that is bound to the triple pattern
 * from which we have derived this query
 * @param lstMembers the abstract query members of the union, flattened if there are embedded unions
 * 
 * @author Franck Michel, I3S laboratory
 */
class AbstractQueryUnion(
    lstMembers: List[AbstractQuery])
        extends AbstractQuery(Set.empty) {

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
        "[" + members.mkString("\n] UNION [\n") + "\n]"
    }

    override def toStringConcrete: String = {
        "[" + members.map(q => q.toStringConcrete).mkString("\n] UNION [\n") + "\n]"
    }

    /**
     * Translate all atomic abstract queries of this abstract query into concrete queries.
     * @param translator the query translator
     */
    override def translateAtomicAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit = {
        for (q <- members)
            q.translateAtomicAbstactQueriesToConcrete(translator)
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
        dataTranslator: MorphBaseDataTranslator): Set[MorphBaseResultRdfTerms] = {

        if (logger.isInfoEnabled) {
            logger.info("===============================================================================");
            logger.info("Generating RDF triples from union query below:\n" + this.toStringConcrete);
        }

        var res = Set[MorphBaseResultRdfTerms]()
        members.foreach(m => { res = res ++ m.generateRdfTerms(dataSourceReader, dataTranslator) })
        logger.info("Union computed " + res.size + " triples.")
        res
    }

    /**
     * Optimize the members of the union
     */
    override def optimizeQuery(optimizer: MorphBaseQueryOptimizer): AbstractQuery = {

        if (!optimizer.selfUnionElimination) return this

        if (members.size == 1) { // security test but abnormal case, should never happen
            logger.warn("Unexpected case: inner join with only one member: " + this.toString)
            return members.head
        }

        if (logger.isDebugEnabled)
            logger.debug("\n------------------ Optimizing query ------------------\n" + this)

        // First, optimize all members individually
        var membersV = members.map(_.optimizeQuery(optimizer))
        if (logger.isDebugEnabled) {
            val res = if (membersV.size == 1) membersV.head else new AbstractQueryUnion(membersV)
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
                    if (left.isInstanceOf[AbstractAtomicQueryMongo] && right.isInstanceOf[AbstractAtomicQueryMongo]) {
                        val opt = left.asInstanceOf[AbstractAtomicQueryMongo].mergeForUnion(right)
                        if (opt.isDefined) {
                            //     i     j     =>   slice(0,i),  merged(i,j),  slice(i+1,j),  slice(j+1, size)
                            // (0, 1, 2, 3, 4) =>   0         ,  merged(1,3),  2           ,  4
                            membersV = membersV.slice(0, i) ++ List(opt.get) ++ membersV.slice(i + 1, j) ++ membersV.slice(j + 1, membersV.size)
                            continue = false
                        }
                    }
                } // end for j
            } // end for i

            if (continue) {
                // There was no change in the last run, we cannot do anymore optimization
                if (membersV.size == 1)
                    return membersV.head
                else
                    return new AbstractQueryUnion(membersV)
            } else {
                // There was a change in this run, let's rerun the optimization with the new list of members
                continue = true
                if (logger.isDebugEnabled) {
                    val res = if (membersV.size == 1) membersV.head else new AbstractQueryUnion(membersV)
                    logger.debug("\n------------------ Query optimized into ------------------\n" + res)
                }
            }
        }

        throw new MorphException("We should not quit the function this way.")
    }
}