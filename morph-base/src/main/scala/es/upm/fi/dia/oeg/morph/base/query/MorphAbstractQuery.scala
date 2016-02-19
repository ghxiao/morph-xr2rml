package es.upm.fi.dia.oeg.morph.base.query

import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator

/**
 * Representation of an abstract query as defined in https://hal.archives-ouvertes.fr/hal-01245883.
 *
 * Also used to store the concrete query (attribute targetQuery) that results from
 * the translation of this abstract query into the target database language.
 *
 * This class is not abstract because it is used as is in the RDB case, in which the original SPARQL-to-SQL query
 * translation method of Morph-RDB has not been changed. Therefore in the RDB case we use this class as an
 * encapsulation of the query rewritten somewhere else.
 *
 * @param boundTriplesMap in the query rewriting context, this is a triples map that is bound to the triple pattern
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

    def toStringConcrete: String = { targetQuery.toString }

    /**
     * Translate all atomic abstract queries of this abstract query into concrete queries.
     *
     * @translator the query translator
     */
    def translateAtomicAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit = {
        throw new MorphException("Not supported")
    }
}