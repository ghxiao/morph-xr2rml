package es.upm.fi.dia.oeg.morph.base

/**
 * @TODO CLASS UNDER RECONSTRUCTON.
 * Ultimately this class should represent all sorts of abstract query operators
 * (INNER/LEFT JOIN, UNION, FILTER  
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