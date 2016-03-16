package es.upm.fi.dia.oeg.morph.abstractquery

import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQuery
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultRdfTerms
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator

/**
 * This class is used as a simple encapsulation of an SQL query in the RDB case, because the original SPARQL-to-SQL query
 * translation method from Morph-RDB has not been updated to deal with abstract queries.
 * Therefore all functions below thrown an exception. Only the targetQuery is used in this class.
 */
class MorphAbstractQuerySql extends MorphAbstractQuery(Set.empty) {

    override def toStringConcrete: String = {
        throw new MorphException("Not supported")
    }

    /**
     * Translate all atomic abstract queries within this abstract query into concrete queries.
     * @param translator the query translator
     */
    override def translateAtomicAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit = {
        throw new MorphException("Not supported")
    }

    /**
     * Check if this atomic abstract queries has a target query properly initialized
     * i.e. targetQuery is not empty
     */
    override def isTargetQuerySet: Boolean = {
        throw new MorphException("Not supported")
    }

    /**
     * Return the list of SPARQL variables projected in this abstract query
     */
    override def getVariables: Set[String] = {
        throw new MorphException("Not supported")
    }

    /**
     * Execute the query and produce the RDF terms for each of the result documents
     * by applying the triples map bound to this query.
     *
     * @param dataSourceReader the data source reader to query the database
     * @param dataTrans the data translator to create RDF terms
     * @return a list of MorphBaseResultRdfTerms instances, one for each result document
     * May return an empty result but NOT null.
     */
    override def generateRdfTerms(
        dataSourceReader: MorphBaseDataSourceReader,
        dataTranslator: MorphBaseDataTranslator): List[MorphBaseResultRdfTerms] = {
        throw new MorphException("Not supported")
    }

    override def optimizeQuery: MorphAbstractQuery = {
        throw new MorphException("Not umplemented")
    }
}