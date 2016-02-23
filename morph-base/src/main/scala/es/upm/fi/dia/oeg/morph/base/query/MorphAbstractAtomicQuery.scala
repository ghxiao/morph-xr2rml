package es.upm.fi.dia.oeg.morph.base.query

import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryCondition
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryProjection
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader

/**
 * Representation of the abstract atomic query as defined in https://hal.archives-ouvertes.fr/hal-01245883
 *
 * @param boundTriplesMap in the query rewriting context, this is a triples map that is bound to the triple pattern
 * from which we have derived this query
 * @param from consists of the triples map logical source
 * @param project set of xR2RML references that shall be projected in the target query, i.e. the references
 * needed to generate the RDF terms of the result triples
 * @param where set of conditions applied to xR2RML references, entailed by matching the triples map
 * with the triple pattern.
 */
class MorphAbstractAtomicQuery(

    boundTriplesMap: Option[R2RMLTriplesMap],
    val from: xR2RMLLogicalSource,
    val project: List[MorphBaseQueryProjection],
    val where: List[MorphBaseQueryCondition])
        extends MorphAbstractQuery(boundTriplesMap) {

    override def toString = {
        val fromStr =
            if (from.docIterator.isDefined)
                from.getValue + ", Iterator: " + from.docIterator
            else
                from.getValue

        "{ from   :  " + fromStr + "\n" +
            "  project: " + project + "\n" +
            "  where  : " + where + " }"
    }

    /**
     * Translate this atomic abstract query into concrete queries.
     * The result is stored in attribute this.targetQuery.
     * @param translator the query translator
     */
    override def translateAtomicAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit = {
        this.targetQuery = translator.atomicAbstractQuerytoConcrete(this)
    }

    /**
     * Check if this atomic abstract queries has a target query properly initialized
     * i.e. targetQuery is not empty
     */
    override def isTargetQuerySet: Boolean = { !targetQuery.isEmpty }

    /**
     * Execute the target database queries against the database and return the result documents.
     *
     * @param dataSourceReader the data source reader
     * @param iter the iterator to apply on query results
     * @return list of instances of MorphBaseResultSet, one for each GenericQuery of targetQuery
     * Must NOT return null, may return an empty result.
     */
    override def executeQuery(dataSourceReader: MorphBaseDataSourceReader, iter: Option[String]): List[MorphBaseResultSet] = {
        this.targetQuery.map(query => dataSourceReader.executeQueryAndIterator(query, iter))
    }

}