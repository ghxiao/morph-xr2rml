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

/**
 * Representation of the UNION abstract query of several abstract queries
 *
 * @param boundTriplesMap in the query rewriting context, this is a triples map that is bound to the triple pattern
 * from which we have derived this query
 * 
 * @param members the abstract query members of the union
 */
class AbstractQueryUnion(
    val members: List[AbstractQuery])
        extends AbstractQuery(Set.empty) {

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

        logger.info("Generating RDF triples from union query below:\n" + this.toStringConcrete);
        val result = members.flatMap(m => m.generateRdfTerms(dataSourceReader, dataTranslator))
        logger.info("Union computed " + result.size + " triples.")
        result
    }

    /**
     * Optimize the members of the union
     */
    override def optimizeQuery(optimizer: MorphBaseQueryOptimizer): AbstractQuery = {

        if (members.size == 1) { // security test but abnormal case, should never happen
            logger.warn("Unexpected case: inner join with only one member: " + this.toString)
            return members.head
        }

        if (logger.isDebugEnabled)
            logger.debug("\n------------------ Optimizing query ------------------\n" + this)

        var membersV = members
        var continue = true

        while (continue) {

            for (i: Int <- 0 to (membersV.size - 2) if continue) {	// from first until second to last (avant-dernier)
                for (j: Int <- (i + 1) to (membersV.size - 1) if continue) { // from i+1 until last

                    val left = membersV(i).optimizeQuery(optimizer)
                    val right = membersV(j).optimizeQuery(optimizer)

                    // Union of 2 atomic queries
                    if (left.isInstanceOf[AbstractAtomicQuery] && right.isInstanceOf[AbstractAtomicQuery]) {
                        val opt = left.asInstanceOf[AbstractAtomicQuery].mergeForUnion(right)
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