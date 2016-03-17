package fr.unice.i3s.morph.xr2rml.mongo.abstractquery

import es.upm.fi.dia.oeg.morph.base.MorphBaseResultRdfTerms
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.query.AbstractQuery
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.exception.MorphException

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
    override def optimizeQuery: AbstractQuery = {

        val membersOpt = members.map(_.optimizeQuery)
        new AbstractQueryUnion(membersOpt)
    }

}