package es.upm.fi.dia.oeg.morph.base.query

import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader

/**
 * Representation of an abstract query as defined in https://hal.archives-ouvertes.fr/hal-01245883.
 *
 * Also used to store the concrete query (attribute targetQuery) that results from
 * the translation of this abstract query into the target database language.
 *
 * <b>Note</b>: This class is not abstract because it is used as is in the RDB case, in which the original SPARQL-to-SQL query
 * translation method of Morph-RDB has not been modified. Therefore in the RDB case we use an instance of this class
 * as an simple encapsulation of the query rewritten somewhere else.
 *
 * @param boundTriplesMap in the query rewriting context, this field is significant only for an instance
 * of MorphAbstractAtomicQuery and it is a triples map that is bound to the triple pattern
 *
 * @author Franck Michel (franck.michel@cnrs.fr)
 */
class MorphAbstractQuery(
        val boundTriplesMap: Option[R2RMLTriplesMap]) {

    /**
     * Result of translating this abstract query into a target database query.
     * This should contain a single query for an RDB.
     * For MongoDB this field is used only for atomic abstract queries, and may
     * contain several queries whose results must be UNIONed.
     */
    var targetQuery: List[GenericQuery] = List.empty

    def setTargetQuery(tq: List[GenericQuery]): MorphAbstractQuery = {
        this.targetQuery = tq
        this
    }

    /**
     * Check if atomic abstract queries within this query have a target query properly initialized
     * i.e. targetQuery is not empty
     */
    def isTargetQuerySet: Boolean = false

    def toStringConcrete: String = { targetQuery.toString }

    /**
     * Translate all atomic abstract queries within this abstract query into concrete queries.
     * @param translator the query translator
     */
    def translateAtomicAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit = {
        throw new MorphException("Not supported")
    }

    /**
     * Execute the target database queries against the database and return the result documents.
     *  
     * @param dataSourceReader the data source reader
     * @param iter the iterator to apply on query results
     * @return list of instances of MorphBaseResultSet, one for each GenericQuery of targetQuery
     * Must NOT return null, may return an empty result.
     */
    def executeQuery(dataSourceReader: MorphBaseDataSourceReader, iter: Option[String]): List[MorphBaseResultSet] = {
        throw new MorphException("Not supported")
    }
}