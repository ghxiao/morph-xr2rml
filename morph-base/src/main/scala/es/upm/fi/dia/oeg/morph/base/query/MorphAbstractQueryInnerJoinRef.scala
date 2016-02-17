package es.upm.fi.dia.oeg.morph.base.query

import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

/**
 * Representation of the INNER JOIN abstract query generated from the relation between a child and a parent triples map.
 *
 * @param boundTriplesMap in the query rewriting context, this is a triples map that is bound to the triple pattern
 * from which we have derived this query
 * @param child the query representing the child triples map
 * @param childRef the xR2RML child reference of the join condition: rr:joinCondition [ ... rr:child ... ]
 * @param parent the query representing the parent triples map
 * @param parentRef the xR2RML parent reference of the join condition: rr:joinCondition [ ... rr:parent ... ]
 */
class MorphAbstractQueryInnerJoinRef(

    boundTriplesMap: Option[R2RMLTriplesMap],

    val child: MorphAbstractAtomicQuery,
    val childRef: String,

    val parent: MorphAbstractAtomicQuery,
    val parentRef: String)

        extends MorphAbstractQuery(boundTriplesMap) {

    override def toString = {
        child.toString + " AS child\n" +
            "INNER JOIN\n" +
            parent.toString + " AS parent\n" +
            "ON child/" + childRef + " = parent/" + parentRef
    }
}