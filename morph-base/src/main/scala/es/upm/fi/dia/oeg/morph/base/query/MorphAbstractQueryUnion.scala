package es.upm.fi.dia.oeg.morph.base.query

import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

/**
 * Representation of the UNION abstract query of several abstract queries
 *
 * @param boundTriplesMap in the query rewriting context, this is a triples map that is bound to the triple pattern
 * from which we have derived this query
 * @members the abstract query members of the union
 */
class MorphAbstractQueryUnion(

    boundTriplesMap: Option[R2RMLTriplesMap],
    val members: List[MorphAbstractQuery])
        extends MorphAbstractQuery(boundTriplesMap) {

    override def toString = {
        members.mkString("\nUNION\n")
    }
}