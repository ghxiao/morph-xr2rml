package fr.unice.i3s.morph.xr2rml.mongo.abstractquery

import org.apache.log4j.Logger

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultRdfTerms
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.AbstractQuery
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryAtomic
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryProjection
import es.upm.fi.dia.oeg.morph.base.query.ConditionType
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery
import es.upm.fi.dia.oeg.morph.base.query.IReference
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryOptimizer
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.TpBindings
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLQuery
import fr.unice.i3s.morph.xr2rml.mongo.MongoDBQuery
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoDataSourceReader
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoDataTranslator
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoResultSet
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeAnd
import fr.unice.i3s.morph.xr2rml.mongo.querytranslator.JsonPathToMongoTranslator
import fr.unice.i3s.morph.xr2rml.mongo.querytranslator.MorphMongoQueryTranslator
import es.upm.fi.dia.oeg.morph.base.query.AbstractCondition
import es.upm.fi.dia.oeg.morph.base.query.AbstractConditionAnd
import es.upm.fi.dia.oeg.morph.base.query.AbstractConditionOr
import es.upm.fi.dia.oeg.morph.base.query.AbstractConditionEquals
import es.upm.fi.dia.oeg.morph.base.query.AbstractConditionNotNull
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoDataTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.TpBindings
import es.upm.fi.dia.oeg.morph.base.querytranslator.TpBinding

/**
 * Representation of the abstract atomic query as defined in https://hal.archives-ouvertes.fr/hal-01245883
 *
 * This specialization implements translation and optimization methods for the case of MongoDB.
 *
 * @author Franck Michel, I3S laboratory
 */
class AbstractQueryAtomicMongo(

    tpBindings: TpBindings,
    from: xR2RMLLogicalSource,
    project: Set[AbstractQueryProjection],
    where: Set[AbstractCondition],
    lim: Option[Long])

        extends AbstractQueryAtomic(tpBindings, from, project, where, lim) {

    /**
     * Translate an atomic abstract query into one or several concrete queries whose results must be UNIONed:<br>
     * 1. the Where part of the atomic abstract query is translated into an abstract
     * MongoDB query using function JsonPathToMongoTranslator.trans().<br>
     * 2. the abstract MongoDB query is optimized and translated into a set of concrete MongoDB queries.<br>
     *
     * The result is stored in attribute 'this.targetQuery'.
     *
     * @param translator the query translator
     */
    override def translateAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit = {
        val mongQTranslator = translator.asInstanceOf[MorphMongoQueryTranslator]

        val whereConds = this.where.filter(c => c.condType != ConditionType.SparqlFilter)
        if (logger.isDebugEnabled()) logger.debug("Translating conditions:" + whereConds)

        // Generate one abstract MongoDB query (MongoQueryNode) for each selected condition
        val mongAbsQs: Set[MongoQueryNode] = whereConds.map(cond => {
            JsonPathToMongoTranslator.trans(cond, this.from.docIterator)
        })

        // If there are more than one query, encapsulate them under a top-level AND
        val mongAbsQ =
            if (mongAbsQs.size > 1) new MongoQueryNodeAnd(mongAbsQs.toList)
            else mongAbsQs.head
        if (logger.isDebugEnabled())
            logger.debug("Conditions translated to abstract MongoDB query:\n" + mongAbsQ)

        // Translate the non-optimized MongoQueryNode instance into one or more optimized concrete MongoDB queries
        val from: MongoDBQuery = MongoDBQuery.parseQueryString(this.from.getValue, this.from.docIterator, true)
        val queries: List[MongoDBQuery] = mongQTranslator.mongoAbstractQuerytoConcrete(from, this.project, mongAbsQ)

        // Generate one GenericQuery for each concrete MongoDB query and assign the result to targetQuery
        this.targetQuery = queries.map(q => new GenericQuery(Constants.DatabaseType.MongoDB, q, this.from.docIterator))
    }

    /**
     * An atomic query cannot be optimized. Return self
     */
    override def optimizeQuery(optimizer: MorphBaseQueryOptimizer): AbstractQuery = { this }

    /**
     * Merge this atomic abstract query with another one in order to perform self-join elimination.
     *
     * Condition: the two queries have the same From part or one is a sub-set of the other, and they are
     * joined on a at least one variable whose reference is declared as unique in the triples map
     * logical source (xrr:uniqueRef).
     * Example:<br>
     * q1.from: <code>db.collection.find({query1})</code>,<br>
     * q2.from: <code>db.collection.find({query1, query2})</code><br>
     * q2.from is a sub-set of q1.from, i.e. q2.from is more specific than q1.from.<br>
     * If q1.project and q2.project contain the same projection "$.field AS ?x", and both
     * logical sources declare property <code>xrr:uniqueRef "$.field"</code>, then this is a self-join
     * that can be eliminated
     *
     * @note The same variable can be projected several times in an atomic query,
     * e.g. when the same variable is used several times in a triple pattern e.g.: "?x ns:predicate ?x .".<br>
     * In addition, a projection can contain several references in case the variable is matched with a template
     * term map, e.g. if ?x is matched with "http://domain/{$.ref1}/{$.ref2.*}", then the projection is:
     * "Set($.ref1, $.ref2.*) AS ?x" => in that case, we must have the same projection in the second query
     * for the merge to be possible.<br>
     * Therefore projection is a set of sets, in the most complex case we may have something like:
     * Set(Set($.ref1, $.ref2.*) AS ?x, Set($.ref3, $.ref4) AS ?x), Set($.ref5, $.ref6) AS ?x))
     *
     * @param q the right query of the join
     * @return an AbstractAtomicQuery if the merge is possible, None otherwise
     */
    def mergeForInnerJoin(q: AbstractQuery): Option[AbstractQueryAtomicMongo] = {

        if (!q.isInstanceOf[AbstractQueryAtomicMongo])
            return None

        val left = this
        val right = q.asInstanceOf[AbstractQueryAtomicMongo]

        val sharedVars = left.getVariables intersect right.getVariables
        val sharedVarsLeftProjections = sharedVars.flatMap(v => left.getProjectionsForVariable(v)).map(_.references)
        val sharedVarsRightProjections = sharedVars.flatMap(v => right.getProjectionsForVariable(v)).map(_.references)

        // Determine if both queries have the same From part (or one is a sub-query of the other),
        // and they are joined on a unique reference. If this is the case, then for sure we can merge them.
        var isJoinOnUniqueRefs = false
        val mostSpec = MongoDBQuery.mostSpecificQuery(left.from, right.from)
        if (mostSpec.isDefined) {
            val sharedUniqueRefs = left.from.uniqueRefs intersect right.from.uniqueRefs
            sharedVars.foreach(x => {
                // We look for at least one shared variable that is projected from that same references in each query,
                // and one of these references is unique in all documents of the database.
                val leftRefs = left.getProjectionsForVariable(x).flatMap(_.references)
                val rightRefs = right.getProjectionsForVariable(x).flatMap(_.references)
                if (leftRefs == rightRefs) {
                    val joinedUniqueRefs = sharedUniqueRefs intersect leftRefs
                    if (joinedUniqueRefs.nonEmpty) {
                        if (logger.isDebugEnabled)
                            logger.debug("Detected join on unique reference(s) " + joinedUniqueRefs)
                        isJoinOnUniqueRefs = true
                    }
                }
            })
        }

        if (isJoinOnUniqueRefs) {
            // ----------------------------- Join on a variable bound to a unique reference ---------------------------
            val mergedBindings = left.tpBindings merge right.tpBindings
            val mergedProj = left.project ++ right.project
            val mergedWhere = left.where ++ right.where
            val result = Some(new AbstractQueryAtomicMongo(mergedBindings, mostSpec.get, mergedProj, mergedWhere, limit))
            result
        } else {
            if (!MongoDBQuery.sameQueries(left.from, right.from)) {
                    logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right + "\nbecause they have different From parts.")
                None
            }

            if (left.where != right.where) {
                if (logger.isTraceEnabled)
                    logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right + "\nbecause they have different Where parts.")
                None
            }

            // If the two queries have no shared variable then this cannot be a self-join
            if (sharedVars.isEmpty) {
                if (logger.isTraceEnabled)
                    logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right + "\nbecause they have no shared variable.")
                None
            }

            if (logger.isTraceEnabled)
                logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right)
            None
        }
    }

    /**
     * Merge this atomic abstract query with another one in order to perform self-join elimination
     * in the context of a referencing object map: join between queries of child and parent triples maps.
     *
     * Condition: the two queries have the same From part or one is a sub-set of the other, and they are
     * joined on a reference declared as unique in the triples map logical source (xrr:UniqueRef).
     * Example:<br>
     * q1.from: <code>db.collection.find({query1})</code>,<br>
     * q2.from: <code>db.collection.find({query1, query2})</code><br>
     * q2.from is a sub-set of q1.from, i.e. q2.from is more specific than q1.from.<br>
     * If the child and parent references are both "$.field", and both logical sources declare property
     * <code>xrr:uniqueRef "$.field"</code>, then this is a self-join.
     *
     * @note This is a simplified version of method mergeForInnerJoin().
     *
     * @param childRef the xR2RML child reference of the join condition: rr:joinCondition [ ... rr:child ... ]
     * @param parent the atomic query representing the parent triples map
     * @param parentRef the xR2RML parent reference of the join condition: rr:joinCondition [ ... rr:parent ... ]
     * @return an AbstractAtomicQuery if the merge is possible, None otherwise
     */
    def mergeForInnerJoinRef(childRef: String,
                             parent: AbstractQueryAtomicMongo,
                             parentRef: String): Option[AbstractQueryAtomicMongo] = {

        val child = this

        // Determine if both queries have the same From part (or one is a sub-query of the other),
        // and they are joined on a unique reference. If this is the case, then for sure we can merge them.
        var isJoinOnUniqueRefs = false
        val mostSpec = MongoDBQuery.mostSpecificQuery(child.from, parent.from)
        if (mostSpec.isDefined) {
            val sharedUniqueRefs = child.from.uniqueRefs intersect parent.from.uniqueRefs
            if (childRef == parentRef)
                if (sharedUniqueRefs contains childRef) {
                    if (logger.isTraceEnabled)
                        logger.trace("Detected join on unique reference(s) " + childRef)
                    isJoinOnUniqueRefs = true
                }
        }

        if (isJoinOnUniqueRefs) {
            // ----------------------------- Join on a a unique reference ---------------------------
            val mergedBindings = child.tpBindings merge parent.tpBindings
            val mergedProj = child.project ++ parent.project
            val mergedWhere = child.where ++ parent.where
            val result = Some(new AbstractQueryAtomicMongo(mergedBindings, mostSpec.get, mergedProj, mergedWhere, limit))
            result
        } else {
            if (!MongoDBQuery.sameQueries(child.from, parent.from)) {
                if (logger.isTraceEnabled)
                    logger.trace("Self-join elimination impossible bewteen \n" + child + "\nand\n" + parent + "\nbecause they have different From parts.")
                None
            }

            if (child.where != parent.where) {
                if (logger.isTraceEnabled)
                    logger.trace("Self-join elimination impossible bewteen \n" + child + "\nand\n" + parent + "\nbecause they have different Where parts.")
                None
            }

            if (logger.isTraceEnabled)
                logger.trace("Self-join elimination impossible bewteen \n" + child + "\nand\n" + parent)
            None
        }
    }

    /**
     * Try to narrow down this atomic query by propagating conditions of the another atomic query
     * that is joined with it (either inner join or a left join).
     * Below we denote by left query 'this' query, and the other query by right query.
     *
     * If the two queries have some shared variables, then Equality and IsNotNull conditions of the right query
     * on those shared variables can be added to the conditions of the left query.
     *
     * @example Assume we have "\$.field1 AS ?x" in left and "\$.field2 AS ?x" in right.
     * If the right query has a Where condition <code>Equals(\$.field2, "value")</code>, then we can add a new condition
     * to the Where conditions of the left query: <code>Equals(\$.field1, "value")</code>
     *
     * @param q the right query of the join
     * @return an optimized version of 'this', based on the query in parameter, or 'this' is no optimization was possible.
     */
    def propagateConditionFromJoinedQuery(q: AbstractQuery): AbstractQueryAtomicMongo = {

        if (!q.isInstanceOf[AbstractQueryAtomicMongo])
            return this

        val left = this
        val right = q.asInstanceOf[AbstractQueryAtomicMongo]
        val sharedVars = left.getVariables intersect right.getVariables

        // Check if some Where conditions of the right query can be added to the Where conditions of the left query to narrow it down
        var condsToReport = Set[AbstractCondition]()
        sharedVars.foreach(x => {
            // Get all the references associated with variable ?x in the left and right queries.
            // Restriction: we deal only with single reference i.e. "ref AS ?x", but not references such as "(ref1, ref2) AS ?x"
            val leftRefs = left.getProjectionsForVariable(x).filter(p => p.references.size == 1).map(_.references.head)
            val rightRefs = right.getProjectionsForVariable(x).filter(p => p.references.size == 1).map(_.references.head)

            rightRefs.foreach(ref => {
                // Select the Where conditions in the right query, of type isNotNull or Equals, 
                // that refer to the right reference ref associated with ?x
                val rightConds = right.where.filter(w => w.hasReference && w.asInstanceOf[IReference].reference == ref)
                rightConds.foreach(rightCond => {
                    rightCond match {
                        case eq: AbstractConditionEquals => {
                            leftRefs.foreach(leftRef => {
                                condsToReport += new AbstractConditionEquals(leftRef, eq.eqValue)
                            })
                        }
                        case nn: AbstractConditionNotNull => {
                            leftRefs.foreach(leftRef => {
                                condsToReport += new AbstractConditionNotNull(leftRef)
                            })
                        }
                    }
                })
            })
        })

        if (condsToReport.nonEmpty) {
            val newLeft = new AbstractQueryAtomicMongo(left.tpBindings, left.from, left.project, left.where ++ condsToReport, limit)
            if (logger.isDebugEnabled) {
                if (newLeft != left)
                    logger.debug("Propagated condition from \n" + right + "\nto\n" + left + "\n producing new optimized query:\n" + newLeft)
            }
            newLeft
        } else
            left
    }

    /**
     * Merge this atomic abstract query with another one in order to perform self-union elimination.
     * The merge is allowed if and only if both queries have the same From part (the logical source).
     *
     * The resulting query Q merges queries Q1 and Q2 this way:<br>
     * (i) the Project part of Q is simply the union of the two sets of projections.<br>
     * (ii) the Where part of Q is an OR of the 2 Where parts: OR(Q1.where, Q2.where).
     * When there are more than one condition in a Where, we embed them in an AND condition, we obtain:
     * OR(AND(Q1.where), AND(Q2.where)).
     *
     * @param q the right query of the union
     * @return an AbstractAtomicQuery if the merge is possible, None otherwise
     */
    def mergeForUnion(q: AbstractQuery): Option[AbstractQueryAtomicMongo] = {

        if (!q.isInstanceOf[AbstractQueryAtomicMongo])
            return None

        val right = q.asInstanceOf[AbstractQueryAtomicMongo]
        var result: Option[AbstractQueryAtomicMongo] = None
        val left = this

        if (MongoDBQuery.sameQueries(left.from, right.from)) {
            val mergedBindings = left.tpBindings merge right.tpBindings
            val mergedProj = left.project ++ right.project
            val leftOr = if (left.where.size > 1) AbstractConditionAnd.create(left.where) else left.where.head
            val rightOr = if (right.where.size > 1) AbstractConditionAnd.create(right.where) else right.where.head
            val mergedWhere = AbstractConditionOr.create(Set(leftOr, rightOr))
            result = Some(new AbstractQueryAtomicMongo(mergedBindings, left.from, mergedProj, Set(mergedWhere), limit))
        }
        result
    }

    /**
     * Execute the query(ies) and produce the RDF terms for each of the result documents
     * by applying the triples map bound to this query.
     * If targetQuery contains several queries their result is UNIONed.
     *
     * @param dataSourceReader the data source reader to query the database
     * @param dataTrans the data translator to create RDF terms
     * @return a set of MorphBaseResultRdfTerms instances, one for each result document
     * May return an empty result but NOT null.
     */
    override def generateRdfTerms(
        dataSourceReader: MorphBaseDataSourceReader,
        dataTrans: MorphBaseDataTranslator): List[MorphBaseResultRdfTerms] = {

        val dataTranslator = dataTrans.asInstanceOf[MorphMongoDataTranslator]

        // Cache queries in case we have several bindings for this query. This is different from the cache in the 
        // data source reader: this one is local to the processing of the abstract query, whereas in the data source reader
        // it is global to the application (and thus much more dangerous)
        var executedQueries = Map[String, List[String]]()

        if (tpBindings.isEmpty) {
            val errMsg = "Atomic abstract query with no triple pattern binding " +
                " => this may be child or parent query of a RefObjectMap. Cannot call the generatedRdfTerms method."
            logger.error(errMsg)
            logger.error("Query: " + this.toString)
            throw new MorphException(errMsg)
        }

        // --- Execute the queries of the tagetQuery of this abstract atomic query
        // We want to produce 'limit' solutions, but we don't know exactly how many documents we must retrieve.
        // We assume that at least one solution is generated from each document (which may not always be true, e.g. if
        // a SPARQL FILTER filters out some triples), an approximation is to retrieve 'limit' documents 
        // to produce at least 'limit' triples for each binding, and in turn 'limit' solutions.
        var start = System.currentTimeMillis()
        val iter: Option[String] = from.docIterator
        var mongoRslts = List[String]()
        for (query <- targetQuery) {
            val queryMapId = MorphMongoDataSourceReader.makeQueryMapId(query, iter, limit)
            if (executedQueries.contains(queryMapId)) {
                if (logger.isDebugEnabled()) logger.debug("Returning query results from cache, queryId: " + queryMapId)
                mongoRslts ++= executedQueries(queryMapId)
            } else {
                val res = dataSourceReader.executeQueryAndIterator(query, iter, limit).asInstanceOf[MorphMongoResultSet].resultSet
                executedQueries = executedQueries + (queryMapId -> res)
                // Make a UNION of all the results
                mongoRslts ++= res
            }
        }

        var end = System.currentTimeMillis()
        if (logger.isDebugEnabled()) logger.debug("Atomic query returned " + mongoRslts.size + " results in " + (end - start) + "ms.")

        //--- Loop on all bindings
        start = System.currentTimeMillis()
        val allResultsTriples = tpBindings.getNonEmptyBindings.toList.flatMap(tpb => {

            val tp = tpb.triple
            val boundTMs = tpb.boundTMs.toList

            val perTpResultTriples: List[MorphBaseResultRdfTerms] = boundTMs.flatMap(tm => {

                val subjectAsVariable = if (tp.getSubject.isVariable) Some(tp.getSubject.toString) else None
                val predicateAsVariable = if (tp.getPredicate.isVariable) Some(tp.getPredicate.toString) else None
                val objectAsVariable = if (tp.getObject.isVariable) Some(tp.getObject.toString) else None

                val sm = tm.subjectMap;
                val pom = tm.predicateObjectMaps.head
                if (logger.isDebugEnabled()) logger.debug("Generating RDF terms under binding " + tpb + " for atomic query: \n" + this.toStringConcrete);

                // ----------------------------------------------------------------
                // --- Main loop: iterate and process each result document of the result set
                // ----------------------------------------------------------------

                var i = 0
                val perTMResultTriples: List[MorphBaseResultRdfTerms] = mongoRslts.flatMap(document => {
                    try {
                        i = i + 1;
                        if (logger.isTraceEnabled()) logger.trace("Generating RDF terms for document " + i + "/" + mongoRslts.size + ": " + document)

                        //---- Create the subject resource
                        val subjects = dataTranslator.translateData(sm, document)
                        if (subjects == null) { throw new Exception("null value in the subject triple") }
                        if (logger.isTraceEnabled()) logger.trace("Document " + i + " subjects: " + subjects)

                        //---- Create the list of resources representing subject target graphs
                        val subjectGraphs = sm.graphMaps.flatMap(sgmElement => dataTranslator.translateData(sgmElement, document))
                        if (logger.isTraceEnabled()) logger.trace("Document " + i + " subject graphs: " + subjectGraphs)

                        // ----- Make a list of resources for the predicate map of the predicate-object map
                        val predicates = dataTranslator.translateData(pom.predicateMaps.head, document)
                        if (logger.isTraceEnabled()) logger.trace("Document " + i + " predicates: " + predicates)

                        // ------ Make a list of resources for the object map of the predicate-object map
                        val objects =
                            if (!pom.objectMaps.isEmpty)
                                dataTranslator.translateData(pom.objectMaps.head, document)
                            else List.empty
                        if (logger.isTraceEnabled()) logger.trace("Document " + i + " objects: " + objects)

                        // ----- Create the list of resources representing target graphs mentioned in the predicate-object map
                        val predicateObjectGraphs = pom.graphMaps.flatMap(pogmElement => dataTranslator.translateData(pogmElement, document))
                        if (logger.isTraceEnabled()) logger.trace("Document" + i + " predicate-object map graphs: " + predicateObjectGraphs)

                        // Compute a list of result triples
                        subjects.flatMap(subject => {
                            predicates.flatMap(predicate => {
                                objects.map(objct => {
                                    new MorphBaseResultRdfTerms(subject, subjectAsVariable, predicate, predicateAsVariable, objct, objectAsVariable)
                                })
                            })
                        })
                    } catch {
                        case e: MorphException => {
                            logger.error("Error while translating data of document " + i + ": " + e.getMessage);
                            e.printStackTrace()
                            List() // empty list will be removed by the flat map of results 
                        }
                        case e: Exception => {
                            logger.error("Unexpected error while translating data of document " + i + ": " + e.getCause() + " - " + e.getMessage);
                            e.printStackTrace()
                            List() // empty list will be removed by the flat map of results 
                        }
                    }
                })
                logger.info("Atomic query generated " + perTMResultTriples.size + " RDF triples for binding\n" + tpb)
                perTMResultTriples
            })
            perTpResultTriples
        })

        end = System.currentTimeMillis()
        logger.info("Atomic query generated " + allResultsTriples.size + " RDF triples for all binding in " + (end - start) + " ms.")

        allResultsTriples
    }
}
