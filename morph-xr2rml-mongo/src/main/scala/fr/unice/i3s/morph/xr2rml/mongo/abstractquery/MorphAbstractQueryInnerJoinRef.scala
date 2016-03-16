package fr.unice.i3s.morph.xr2rml.mongo.abstractquery

import org.apache.log4j.Logger
import com.hp.hpl.jena.graph.Triple
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultRdfTerms
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.path.MixedSyntaxPath
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQuery
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoDataTranslator
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoResultSet
import es.upm.fi.dia.oeg.morph.base.querytranslator.TPBinding
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoDataSourceReader
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource

/**
 * Representation of the INNER JOIN abstract query between two atomic abstract queries,
 * generated from the relation between a child and a parent triples map.
 *
 * @param tpBindings a couple (triple pattern, triples map) for which we create this query.
 * May contain several bindings after query optimization e.g. self-join elimination => 2 merged atomic queries
 * @param child the query representing the child triples map
 * @param childRef the xR2RML child reference of the join condition: rr:joinCondition [ ... rr:child ... ]
 * @param parent the query representing the parent triples map
 * @param parentRef the xR2RML parent reference of the join condition: rr:joinCondition [ ... rr:parent ... ]
 */
class MorphAbstractQueryInnerJoinRef(

    tpBindings: Set[TPBinding],
    val child: MorphAbstractAtomicQuery,
    val childRef: String,
    val parent: MorphAbstractAtomicQuery,
    val parentRef: String)

        extends MorphAbstractQuery(tpBindings) {

    val logger = Logger.getLogger(this.getClass().getName());

    override def equals(a: Any): Boolean = {
        a.isInstanceOf[MorphAbstractQueryInnerJoinRef] && {
            val q = a.asInstanceOf[MorphAbstractQueryInnerJoinRef]
            this.child == q.child && this.childRef == q.childRef && this.parent == q.parent && this.parentRef == q.parentRef
        }
    }

    override def toString = {
        val bdgs = if (tpBindings.nonEmpty) tpBindings.mkString(", ") + "\n" else ""
        bdgs +
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
    override def translateAtomicAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit = {
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
     * Return the list of SPARQL variables projected in this abstract query
     */
    override def getVariables: Set[String] = {
        (child.getVariables ++ parent.getVariables)
    }

    /**
     * Execute the query and produce the RDF terms for each of the result documents
     * by applying the triples map bound to this query.
     *
     * Each of the child and parent queries is executed. If their "targetQuery" contains
     * several queries their result is UNIONed.
     *
     * @param dataSourceReader the data source reader to query the database
     * @param dataTrans the data translator to create RDF terms
     * @return a list of MorphBaseResultRdfTerms instances, one for each result document
     * May return an empty result but NOT null.
     * @throws MorphException if the triples map bound to the query has no referencing object map
     */
    override def generateRdfTerms(
        dataSourceReader: MorphBaseDataSourceReader,
        dataTrans: MorphBaseDataTranslator): List[MorphBaseResultRdfTerms] = {

        // Cache queries in case we have several bindings for this query
        val executedQueries: scala.collection.mutable.Map[String, List[String]] = new scala.collection.mutable.HashMap

        val total = this.tpBindings.toList.flatMap(tpb => {
            val triple = tpb.tp
            val tm = tpb.bound

            val subjectAsVariable =
                if (triple.getSubject.isVariable)
                    Some(triple.getSubject.toString)
                else None
            val predicateAsVariable =
                if (triple.getPredicate.isVariable)
                    Some(triple.getPredicate.toString)
                else None
            val objectAsVariable =
                if (triple.getObject.isVariable)
                    Some(triple.getObject.toString)
                else None

            val dataTranslator = dataTrans.asInstanceOf[MorphMongoDataTranslator]
            val sm = tm.subjectMap;
            val pom = tm.predicateObjectMaps.head
            val iter: Option[String] = tm.logicalSource.docIterator
            logger.info("Generating RDF triples from inner join Ref query under triples map " + tm.toString + ": \n" + this.toStringConcrete);

            // Execute the child queries
            val childResSets = child.targetQuery.map(query => {
                val queryMapId = MorphMongoDataSourceReader.makeQueryMapId(query, iter)
                if (executedQueries.contains(queryMapId)) {
                    logger.info("Returning query results from cache.")
                    executedQueries(queryMapId)
                } else {
                    val resultSet = dataSourceReader.executeQueryAndIterator(query, iter).asInstanceOf[MorphMongoResultSet].resultSet
                    executedQueries += (queryMapId -> resultSet)
                    resultSet
                }
            })
            // Make a UNION (flatten) of all the child results
            val childResultSet = childResSets.flatten
            logger.info("Query returned " + childResultSet.size + " results.")

            // Execute the parent queries (in the parent triples map), apply the iterator, and make a UNION (flatMap) of the results
            val parentResultSet = {
                if (!pom.refObjectMaps.isEmpty) {
                    val rom = pom.refObjectMaps.head
                    val parentTM = dataSourceReader.factory.getMappingDocument.getParentTriplesMap(rom)

                    // Execute the parent queries 
                    val parentResSets = parent.targetQuery.map(query => {
                        val queryMapId = MorphMongoDataSourceReader.makeQueryMapId(query, parentTM.logicalSource.docIterator)
                        if (executedQueries.contains(queryMapId)) {
                            logger.info("Returning query results from cache.")
                            executedQueries(queryMapId)
                        } else {
                            val resultSet = dataSourceReader.executeQueryAndIterator(query, parentTM.logicalSource.docIterator).asInstanceOf[MorphMongoResultSet].resultSet
                            executedQueries += (queryMapId -> resultSet)
                            resultSet
                        }
                    })
                    // Make a UNION (flatten) of all the parent results
                    val parentResultSet = parentResSets.flatten
                    logger.info("Query returned " + parentResultSet.size + " results.")
                    parentResultSet
                } else
                    throw new MorphException("Error: inner join ref query bound to a triples map that has no RefObjectMap.")
            }

            // Main loop: iterate and process each result document of the result set
            var i = 0;
            val terms = for (document <- childResultSet) yield {
                try {
                    i = i + 1;
                    if (logger.isDebugEnabled()) logger.debug("Generating RDF terms for child document " + i + "/" + childResultSet.size + ": " + document)

                    //---- Create the subject resource
                    val subjects = dataTranslator.translateData(sm, document)
                    if (subjects == null) { throw new Exception("null value in the subject triple") }
                    if (logger.isDebugEnabled()) logger.debug("Document " + i + " subjects: " + subjects)

                    //---- Create the list of resources representing subject target graphs
                    val subjectGraphs = sm.graphMaps.flatMap(sgmElement => {
                        dataTranslator.translateData(sgmElement, document)
                    })
                    if (logger.isTraceEnabled()) logger.trace("Document " + i + " subject graphs: " + subjectGraphs)

                    // ----- Make a list of resources for the predicate map of the predicate-object map
                    val predicates = dataTranslator.translateData(pom.predicateMaps.head, document)
                    if (logger.isTraceEnabled()) logger.trace("Document " + i + " predicates: " + predicates)

                    // ------ Make a list of resources for the referencing object map:
                    // Generate the IRIs from the parent query using the subject map of the parent triples map, 
                    // and join the child and parent references in the join condition
                    val refObjects = {
                        if (!pom.refObjectMaps.isEmpty) {

                            // ---- Compute a list of subject IRIs for the join condition
                            val rom = pom.refObjectMaps.head
                            val parentTM = dataTranslator.factory.getMappingDocument.getParentTriplesMap(rom)
                            val joinCond = rom.joinConditions.head

                            // Evaluate the child reference on the current document (of the child triples map)
                            val childMsp = MixedSyntaxPath(joinCond.childRef, sm.refFormulaion)
                            val childValues: List[Object] = childMsp.evaluate(document)

                            // Evaluate the parent reference on each parent query result. The result is stored as pairs:
                            // (JSON document, result of the evaluation of the parent reference on the JSON document)
                            val parentMsp = MixedSyntaxPath(joinCond.parentRef, parentTM.logicalSource.refFormulation)
                            val parentValues = parentResultSet.map(res => (res, parentMsp.evaluate(res)))

                            // ---- Make the join between the child and parent values
                            val parentSubjects = parentValues.flatMap(parentVal => {
                                // For each document returned by the parent query (named parent document),
                                // if at least one of the child values is in the current parent document values, 
                                // then generate an RDF term for the subject of the current parent document.
                                if (!childValues.intersect(parentVal._2).isEmpty) // parentVal._2 is the evaluation of the parent ref
                                    Some(dataTranslator.translateData(parentTM.subjectMap, parentVal._1)) // parentVal._1 is the JSON document itself
                                else
                                    // There was no match: return an empty list so that the final intersection of candidate return nothing
                                    Some(List())
                            }).flatten
                            if (logger.isTraceEnabled()) logger.trace("Join parent candidates: " + joinCond.toString + ", result:" + parentSubjects)

                            // Optionally convert the result to an RDF collection or container
                            if (rom.isR2RMLTermType)
                                parentSubjects
                            else
                                dataTranslator.createCollection(rom.termType.get, parentSubjects)
                        } else
                            List.empty
                    }
                    if (logger.isTraceEnabled()) logger.trace("Document " + i + " refObjects: " + refObjects)

                    // ----- Create the list of resources representing target graphs mentioned in the predicate-object map
                    val predicateObjectGraphs = pom.graphMaps.flatMap(pogmElement => {
                        dataTranslator.translateData(pogmElement, document)
                    });
                    if (logger.isTraceEnabled()) logger.trace("Document" + i + " predicate-object map graphs: " + predicateObjectGraphs)

                    // Result 
                    Some(new MorphBaseResultRdfTerms(
                        subjects, subjectAsVariable,
                        predicates, predicateAsVariable,
                        refObjects, objectAsVariable,
                        (subjectGraphs ++ predicateObjectGraphs).toList))
                } catch {
                    case e: MorphException => {
                        logger.error("Error while translating data of document " + i + ": " + e.getMessage);
                        e.printStackTrace()
                        None
                    }
                    case e: Exception => {
                        logger.error("Unexpected error while translating data of document " + i + ": " + e.getCause() + " - " + e.getMessage);
                        e.printStackTrace()
                        None
                    }
                }
            }
            terms.flatten // get rid of the None's (in case there was an exception)
        })
        logger.info("Inner join Ref computed " + total.size + " triples.")
        total
    }

    /**
     * Try to merge the the child and parent queries
     */
    override def optimizeQuery: MorphAbstractQuery = {
        val opt = child.mergeForInnerJoin(this.parent)
        opt.getOrElse(this)
    }
}