package es.upm.fi.dia.oeg.morph.base

/**
 * @TODO CLASS UNDER RECONSTRUCTON. NO MORE CHILD NOR PARENT QUERY.
 *
 * <p>This class represents a union of concrete queries applying either to the child or parent queries,
 * and a set of join conditions in the case the database cannot compute them.</p>
 *
 * <p>In the RDB case, the UnionOfGenericQueries is a bit exaggerated ;-): it should contain only
 * a child query (there is no need to split child and parent queries since SQL supports joins),
 * and exactly one element in the child query (since SQL supports the UNION).</p>
 *
 * <p>Conversely, MongoDB does not support the JOIN, therefore there may be a child <em>and</em> a parent query,
 * as well a set set of join conditions to process afterwards.
 * Besides, MongoDB does support UNIONs (by means of the $or operator), but only if there is no $where as members of the $or.
 * Therefore it is not always possible to create a MongoDB query that is equivalent to the SPARQL query.
 * In this case, several concrete queries are returned, and the xR2RML processor shall execute them independently
 * and compute the union.</p>
 */
class AbstractQuery(
        val queries: List[GenericQuery]) {

    def head = queries.head

    override def toString(): String = { "queries: " + queries }
}

object AbstractQuery {
    def apply() = new AbstractQuery(List.empty)
    def apply(query: GenericQuery) = new AbstractQuery(List(query))
    def apply(queries: List[GenericQuery]) = new AbstractQuery(queries)
}