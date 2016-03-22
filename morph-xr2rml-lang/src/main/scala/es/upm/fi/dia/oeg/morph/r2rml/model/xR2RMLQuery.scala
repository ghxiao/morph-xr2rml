package es.upm.fi.dia.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.Constants

class xR2RMLQuery(
    val query: String,
    refFormulation: String,
    iterator: Option[String])
        extends xR2RMLLogicalSource(Constants.LogicalTableType.QUERY, refFormulation, iterator) {

    private def cleanString(str: String) = str.trim.replaceAll("\\s", "")

    /**
     * Return true if both xR2RMLQueries have the same query, reference formulation and iterator.
     * 
     * @todo Improve the comparison to take into account queries with same semantic despite 
     * a different order of sub-queries.
     * E.g. {'p': {$eq 5}, 'q': {$eq 6}} == {'q': {$eq 6}, 'p': {$eq 5}}
     */
    override def equals(q: Any): Boolean = {
        q.isInstanceOf[xR2RMLQuery] && {
            val ls = q.asInstanceOf[xR2RMLQuery]
            this.logicalTableType == ls.logicalTableType && this.refFormulation == ls.refFormulation &&
                this.docIterator == ls.docIterator && cleanString(this.query) == cleanString(ls.query)
        }
    }

    override def getValue(): String = { this.query; }
}
