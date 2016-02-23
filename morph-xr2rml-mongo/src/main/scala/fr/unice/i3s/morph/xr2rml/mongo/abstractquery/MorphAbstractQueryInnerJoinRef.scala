package fr.unice.i3s.morph.xr2rml.mongo.abstractquery

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoResultSet
import fr.unice.i3s.morph.xr2rml.mongo.querytranslator.MorphMongoQueryTranslator

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

    override def toStringConcrete: String = {
        child.toStringConcrete + " AS child\n" +
            "INNER JOIN\n" +
            parent.toStringConcrete + " AS parent\n" +
            "ON child/" + childRef + " = parent/" + parentRef
    }

    /**
     * Translate all atomic abstract queries of this abstract query into concrete queries.
     * @param translator the query translator
     */
    def translateAtomicAbstactQueriesToConcrete(translator: MorphMongoQueryTranslator): Unit = {
        child.translateAtomicAbstactQueriesToConcrete(translator)
        parent.translateAtomicAbstactQueriesToConcrete(translator)
    }

    /**
     * Check if atomic abstract queries within this query have a target query properly initialized
     * i.e. targetQuery is not empty
     */
    override def isTargetQuerySet: Boolean = {
        child.isTargetQuerySet && parent.isTargetQuerySet
    }

    /**
     * Execute the target database queries against the database and return a single result set that
     * contains a UNION of all JSON results of all queries in targetQuery
     *
     * @param dataSourceReader the data source reader
     * @param iter the iterator to apply on query results
     * @return two result sets: one contains the UNION of the results of the child queries, the other of the parent queries
     * Must NOT return null, may return an empty result.
     */
    def executeQuery(dataSourceReader: MorphBaseDataSourceReader, iter: Option[String]): (MorphMongoResultSet, Option[MorphMongoResultSet]) = {

        val tm = boundTriplesMap.get
        val pom = tm.predicateObjectMaps.head

        // Execute the child queries and create a MorphMongoResultSet with a UNION (flatMap) of all the results
        val childResSets = child.targetQuery.map(query => dataSourceReader.executeQueryAndIterator(query, iter))
        val childRes = childResSets.flatMap(res => res.asInstanceOf[MorphMongoResultSet].resultSet)
        val childResultSet = new MorphMongoResultSet(childRes)

        // Execute the parent queries (in the join condition), apply the iterator, and make a UNION (flatMap) of the results
        val parentResultSet = {
            if (!pom.refObjectMaps.isEmpty) {
                val rom = pom.refObjectMaps.head
                val parentTM = dataSourceReader.md.getParentTriplesMap(rom)

                // Execute the parent queries and create a MorphMongoResultSet with a UNION (flatMap) of all the results
                val parentResSets = parent.targetQuery.map(query => dataSourceReader.executeQueryAndIterator(query, parentTM.logicalSource.docIterator))
                val parentRes = parentResSets.flatMap(res => res.asInstanceOf[MorphMongoResultSet].resultSet)

                Some(new MorphMongoResultSet(parentRes))
            } else
                None
        }

        (childResultSet, parentResultSet)
    }
}