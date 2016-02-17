package es.upm.fi.dia.oeg.morph.base.query

import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

/**
 * Representation of an abstract query as defined in https://hal.archives-ouvertes.fr/hal-01245883
 *
 * @param boundTriplesMap in the query rewriting context, this is a triples map that is bound to the triple pattern
 */
class MorphAbstractQuery(
        val boundTriplesMap: Option[R2RMLTriplesMap]) {

    /**
     *  Result of translating this abstract query into a target database query.
     *  This should contain a single query for an RDB. For MongoDB it may contain several queries
     *  whose results must be unioned (merged) to get the query result.
     */
    var targetQuery: List[GenericQuery] = List.empty

    def setTargetQuery(tq: List[GenericQuery]): MorphAbstractQuery = {
        this.targetQuery = tq
        this
    }
}