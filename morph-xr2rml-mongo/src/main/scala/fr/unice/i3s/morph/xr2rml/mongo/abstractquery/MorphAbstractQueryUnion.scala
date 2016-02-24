package fr.unice.i3s.morph.xr2rml.mongo.abstractquery

import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQuery
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultRdfTerms
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoDataTranslator
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoResultSet
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.Constants

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

    override def toStringConcrete: String = {
        members.map(q => q.toStringConcrete).mkString("\nUNION\n")
    }

    /**
     * Translate all atomic abstract queries of this abstract query into concrete queries.
     * @param translator the query translator
     */
    override def translateAtomicAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit = {
        for (q <- members)
            q.translateAtomicAbstactQueriesToConcrete(translator)
    }

    /**
     * Check if atomic abstract queries within this query have a target query properly initialized
     * i.e. targetQuery is not empty
     */
    override def isTargetQuerySet: Boolean = {
        var res: Boolean = false
        if (!members.isEmpty)
            res = members.head.isTargetQuerySet
        for (q <- members)
            res = res && q.isTargetQuerySet
        res
    }

    /**
     * Execute each query of the union and produce the RDF terms for each of the result documents
     * by applying the triples map bound to this query.
     * then make a UNION of all the results
     *
     * @param dataSourceReader the data source reader to query the database
     * @param dataTrans the data translator to create RDF terms
     * @return a list of MorphBaseResultRdfTerms instances, one for each result document of each member query.
     * May return an empty result but NOT null.
     */
    override def generateRdfTerms(
        dataSourceReader: MorphBaseDataSourceReader,
        dataTranslator: MorphBaseDataTranslator): List[MorphBaseResultRdfTerms] = {

        members.flatMap(m => m.generateRdfTerms(dataSourceReader, dataTranslator))
    }
}