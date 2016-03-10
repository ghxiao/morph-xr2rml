package es.upm.fi.dia.oeg.morph.base.query

import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultRdfTerms

/**
 * Representation of an abstract query as defined in https://hal.archives-ouvertes.fr/hal-01245883.
 *
 * Also used to store the concrete query (attribute targetQuery) that results from
 * the translation of this abstract query into the target database language.
 *
 * @param boundTriplesMap in the query rewriting context, this field is significant only for an instance
 * of MorphAbstractAtomicQuery and it is a triples map that is bound to the triple pattern
 *
 * @author Franck Michel (franck.michel@cnrs.fr)
 */
abstract class MorphAbstractQuery(
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

    def toStringConcrete: String = { targetQuery.toString }

    //---- Abstract methods ----

    /**
     * Check if atomic abstract queries within this query have a target query properly initialized
     * i.e. targetQuery is not empty
     */
    def isTargetQuerySet: Boolean

    /**
     * Translate all atomic abstract queries within this abstract query into concrete queries.
     * @param translator the query translator
     */
    def translateAtomicAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit

    /**
     * Return the list of SPARQL variables projected in this abstract query
     */
    def getVariables: List[String]

    /**
     * Execute the query and produce the RDF terms for each of the result documents
     * by applying the triples map bound to this query.
     *
     * @param dataSourceReader the data source reader to query the database
     * @param dataTrans the data translator to create RDF terms
     * @return a list of MorphBaseResultRdfTerms instances, one for each result document
     * May return an empty result but NOT null.
     */
    def generateRdfTerms(
        dataSourceReader: MorphBaseDataSourceReader,
        dataTranslator: MorphBaseDataTranslator): List[MorphBaseResultRdfTerms]

    /**
     * Misc optimizations of the abstract query, notation self-join eliminations
     */
    def optimizeQuery: MorphAbstractQuery
}