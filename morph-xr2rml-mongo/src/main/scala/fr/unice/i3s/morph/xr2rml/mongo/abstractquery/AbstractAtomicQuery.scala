package fr.unice.i3s.morph.xr2rml.mongo.abstractquery

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultRdfTerms
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.AbstractQuery
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryCondition
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionEquals
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryProjection
import es.upm.fi.dia.oeg.morph.base.query.ConditionType
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery
import es.upm.fi.dia.oeg.morph.base.query.IReference
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.TPBinding
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import fr.unice.i3s.morph.xr2rml.mongo.MongoDBQuery
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoDataSourceReader
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoDataTranslator
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoResultSet
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeAnd
import fr.unice.i3s.morph.xr2rml.mongo.querytranslator.JsonPathToMongoTranslator
import fr.unice.i3s.morph.xr2rml.mongo.querytranslator.MorphMongoQueryTranslator
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionAnd
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionOr
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryOptimizer
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionIsNull
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionNotNull

/**
 * Representation of the abstract atomic query as defined in https://hal.archives-ouvertes.fr/hal-01245883
 *
 * @param tpBindings a couple (triple pattern, triples map) for which we create this atomic query.
 * Empty in the case of a child or parent query in a referencing object map, and in this case the binding is
 * in the instance of AbstractQueryInnerJoinRef.
 * tpBindings may contain several bindings after query optimization e.g. self-join elimination i.e. 2 atomic queries are merged
 * into a single one that will be used to generate triples for by 2 triples maps i.e. 2 bindings
 *
 * @param from the logical source, which must be the same as in the triples map of tpBindings
 *
 * @param project set of xR2RML references that shall be projected in the target query, i.e. the references
 * needed to generate the RDF terms of the result triples.<br>
 * Note that the same variable can be projected several times, e.g. when the same variable is used
 * several times in a triple pattern e.g.: "?x ns:predicate ?x ."<br>
 * Besides a projection can contain several references in case the variable is matched with a template
 * term map, e.g. if ?x is matched with "http://domain/{$.ref1}/{$.ref2.*}", then the projection is:
 * "Set($.ref1, $.ref2.*) AS ?x" => in that case, we must have the same projection in the second query
 * for the merge to be possible.<br>
 * Therefore projection is a set of sets, in the most complex case we may have something like:
 * Set(Set($.ref1, $.ref2.*) AS ?x, Set($.ref3, $.ref4) AS ?x), Set($.ref5, $.ref6) AS ?x))
 *
 * @param where set of conditions applied to xR2RML references, entailed by matching the triples map
 * with the triple pattern. If there are more than one condition the semantics is a logical AND of all.
 */
class AbstractAtomicQuery(

    tpBindings: Set[TPBinding],
    val from: xR2RMLLogicalSource,
    val project: Set[AbstractQueryProjection],
    val where: Set[AbstractQueryCondition])

        extends AbstractQuery(tpBindings) {

    val logger = Logger.getLogger(this.getClass().getName())

    override def equals(a: Any): Boolean = {
        a.isInstanceOf[AbstractAtomicQuery] && {
            val p = a.asInstanceOf[AbstractAtomicQuery]
            this.from == p.from && this.project == p.project && this.where == p.where
        }
    }

    override def toString = {
        val fromStr =
            if (from.docIterator.isDefined)
                from.getValue + ", Iterator: " + from.docIterator
            else
                from.getValue

        val bdgs = if (tpBindings.nonEmpty) tpBindings.mkString(" ", ", ", "\n  ") else " "
        "{" + bdgs +
            "from   : " + fromStr + "\n" +
            "  project: " + project + "\n" +
            "  where  : " + where + " }"
    }

    override def toStringConcrete = {
        val bdgs = if (tpBindings.nonEmpty) tpBindings.mkString(", ") + "\n " else ""
        "{ " + bdgs +
            targetQuery.map(_.concreteQuery).mkString("\nUNION\n") + " }"
    }

    /**
     * Translate an atomic abstract query into one or several concrete queries whose results must be UNIONed.
     *
     * First, the atomic abstract query is translated into an abstract MongoDB query using the
     * JsonPathToMongoTranslator.trans() function.<br>
     * Then, the abstract MongoDB query is translated into a set of concrete MongoDB queries
     * by function mongoAbstractQuerytoConcrete().
     *
     * The result is stored in attribute this.targetQuery.
     *
     * @param translator the query translator
     * @return none, the result is stored in attribute this.targetQuery.
     */
    override def translateAtomicAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit = {
        val mongQTranslator = translator.asInstanceOf[MorphMongoQueryTranslator]

        val whereConds = this.where.filter(c => c.condType != ConditionType.SparqlFilter)
        if (logger.isDebugEnabled()) logger.debug("Translating conditions:" + whereConds)

        // Generate one abstract MongoDB query (MongoQueryNode) for each selected condition
        val mongAbsQs: Set[MongoQueryNode] = whereConds.map(cond => {
            if (cond.hasReference) {
                val condIRef = cond.asInstanceOf[IReference]
                // If there is an iterator, replace the heading "$" of the JSONPath reference with the iterator path
                val iter = this.from.docIterator
                if (iter.isDefined)
                    condIRef.reference = condIRef.reference.replace("$", iter.get)

                //--- Translate the condition on a JSONPath reference into an abstract MongoDB query
                JsonPathToMongoTranslator.trans(condIRef.asInstanceOf[AbstractQueryCondition])
            } else {
                //--- The condition type is IsNull or Or
                // @todo if the iterator is defined, add it to references of inner conditions
                JsonPathToMongoTranslator.trans(cond)
            }
        })

        // If there are more than one query, encapsulate them under a top-level AND
        val mongAbsQ =
            if (mongAbsQs.size > 1) new MongoQueryNodeAnd(mongAbsQs.toList)
            else mongAbsQs.head
        if (logger.isDebugEnabled())
            logger.debug("Conditions translated to abstract MongoDB query:\n" + mongAbsQ)

        // Create the concrete query/queries from the set of abstract MongoDB queries
        val from: MongoDBQuery = MongoDBQuery.parseQueryString(this.from.getValue, this.from.docIterator, true)
        val queries: List[MongoDBQuery] = mongQTranslator.mongoAbstractQuerytoConcrete(from, this.project, mongAbsQ)

        // Generate one GenericQuery for each concrete MongoDB query and assign the result as the target query
        this.targetQuery = queries.map(q => new GenericQuery(Constants.DatabaseType.MongoDB, q, this.from.docIterator))
    }

    /**
     * Check if this atomic abstract queries has a target query properly initialized
     * i.e. targetQuery is not empty
     */
    override def isTargetQuerySet: Boolean = { !targetQuery.isEmpty }

    /**
     * Return the set of SPARQL variables projected in this abstract query
     */
    override def getVariables: Set[String] = {
        project.filter(_.as.isDefined).map(_.as.get)
    }

    /**
     * Execute the query and produce the RDF terms for each of the result documents
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
        dataTrans: MorphBaseDataTranslator): Set[MorphBaseResultRdfTerms] = {

        // Cache queries in case we have several bindings for this query
        var executedQueries = Map[String, List[String]]()

        if (tpBindings.isEmpty) {
            val errMsg = "Atomic abstract query with no triple pattern binding " +
                " => this is a child or parent query of a RefObjectMap. Cannot call the generatedRdfTerms method."
            logger.error(errMsg)
            logger.error("Query: " + this.toString)
            throw new MorphException(errMsg)
        }

        val resultSetAll: Set[MorphBaseResultRdfTerms] = tpBindings.flatMap(tpb => {
            val tp = tpb.tp
            val subjectAsVariable =
                if (tp.getSubject.isVariable)
                    Some(tp.getSubject.toString)
                else None
            val predicateAsVariable =
                if (tp.getPredicate.isVariable)
                    Some(tp.getPredicate.toString)
                else None
            val objectAsVariable =
                if (tp.getObject.isVariable)
                    Some(tp.getObject.toString)
                else None

            val dataTranslator = dataTrans.asInstanceOf[MorphMongoDataTranslator]
            val tm = tpb.bound
            val sm = tm.subjectMap;
            val pom = tm.predicateObjectMaps.head
            val iter: Option[String] = tm.logicalSource.docIterator
            logger.info("Generating RDF terms under triples map " + tm.toString + " for atomic query: \n" + this.toStringConcrete);

            // Execute the queries of tagetQuery
            var resultSet = List[String]()
            this.targetQuery.foreach(query => {
                val queryMapId = MorphMongoDataSourceReader.makeQueryMapId(query, iter)
                if (executedQueries.contains(queryMapId)) {
                    logger.info("Returning query results from cache.")
                    resultSet ++= executedQueries(queryMapId)
                } else {
                    val res = dataSourceReader.executeQueryAndIterator(query, iter).asInstanceOf[MorphMongoResultSet].resultSet
                    executedQueries = executedQueries + (queryMapId -> res)
                    // Make a UNION of all the results
                    resultSet ++= res
                }
            })
            logger.info("Query returned " + resultSet.size + " results.")

            // Main loop: iterate and process each result document of the result set
            var i = 0;
            val terms = for (document: String <- resultSet) yield {
                try {
                    i = i + 1;
                    if (logger.isTraceEnabled()) logger.trace("Generating RDF terms for document " + i + "/" + resultSet.size + ": " + document)

                    //---- Create the subject resource
                    val subjects = dataTranslator.translateData(sm, document)
                    if (subjects == null) { throw new Exception("null value in the subject triple") }
                    if (logger.isTraceEnabled()) logger.trace("Document " + i + " subjects: " + subjects)

                    //---- Create the list of resources representing subject target graphs
                    val subjectGraphs = sm.graphMaps.flatMap(sgmElement => {
                        dataTranslator.translateData(sgmElement, document)
                    })
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
                    val predicateObjectGraphs = pom.graphMaps.flatMap(pogmElement => {
                        dataTranslator.translateData(pogmElement, document)
                    });
                    if (logger.isTraceEnabled()) logger.trace("Document" + i + " predicate-object map graphs: " + predicateObjectGraphs)

                    // Result 
                    Some(new MorphBaseResultRdfTerms(
                        subjects, subjectAsVariable,
                        predicates, predicateAsVariable,
                        objects, objectAsVariable,
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
            val result = terms.flatten // get rid of the None's (in case there was an exception)
            logger.info("Atomic query computed " + result.size + " triples for binding " + tpb)
            result.toSet
        })
        resultSetAll
    }

    /**
     * An atomic query cannot be optimized. Return self
     */
    override def optimizeQuery(optimizer: MorphBaseQueryOptimizer): AbstractQuery = { this }

    /**
     * Merge this atomic abstract query with another one in order to perform self-join elimination.
     * The merge is allowed if and only if 3 conditions are met:<br>
     * (i) both queries have the same From part (the logical source), or one is more specific than
     * the other (in that case we keep the most specific one).<br>
     * (ii) they have at least one shared variable (on which the join is to be done) (see 1st and 2nd example);<br>
     * (iii) the shared variables are projected from the same xR2RML reference(s) in both queries (see 1st and 2nd example);<br>
     * (iv) two non-shared variables must not be projected from the same xR2RML reference(s) (see 3rd example);
     *
     * @example if Q1 and Q2 have the same logical source, and they have a shared variable ?x,
     * then they need to project the same reference as ?x: "$.fieldName AS ?x".
     * If we have "$.field1 AS ?x" in Q1 "$.field2 AS ?x" is Q2, then this is not a self-join,
     * on the contrary this is a regular join.
     *
     * @example If Q1 and Q2 have the same logical source, and<br>
     * Q1 has projections "$.a AS ?x", "$.b AS ?x",<br>
     * Q2 has projections "$.a AS ?x", "$.c AS ?x"<br>
     * then this is a proper self-join because the same reference "$.a" is projected as ?x in both queries.
     * i.e. In general, for each shared variable ?x, there should be at least one common projection of ?x.
     *
     * @example In a graph pattern like this:<br>
     * <code>?x prop ?y. ?x prop ?z. FILTER (?y != ?z) </code><br>,
     * we may obtain Q1 and Q2 with the same logical source,<br>
     * Q1 has projections "$.x AS ?x", "$.a AS ?y",<br>
     * Q2 has projections "$.x AS ?x", "$.a AS ?z".<br>
     * then this is not a self-join because the same reference "$.a" is projected as two different variables.
     *
     * Note that the same variable can be projected several times, e.g. when the same variable is used
     * several times in a triple pattern e.g.: "?x ns:predicate ?x ."
     *
     * Besides a projection can contain several references in case the variable is matched with a template
     * term map, e.g. if ?x is matched with "http://domain/{$.ref1}/{$.ref2.*}", then the projection is:
     * "Set($.ref1, $.ref2.*) AS ?x" => in that case, we must have the same projection in the second query
     * for the merge to be possible.
     *
     * Therefore projection is a set of sets, in the most complex case we may have something like:
     * Set(Set($.ref1, $.ref2.*) AS ?x, Set($.ref3, $.ref4) AS ?x), Set($.ref5, $.ref6) AS ?x))
     *
     * @param q the right query of the join
     * @return an AbstractAtomicQuery if the merge is possible, None otherwise
     */
    def mergeForInnerJoin(q: AbstractQuery): Option[AbstractAtomicQuery] = {

        if (!q.isInstanceOf[AbstractAtomicQuery])
            return None

        val left = this
        val right = q.asInstanceOf[AbstractAtomicQuery]
        var result: Option[AbstractAtomicQuery] = None

        val mostSpec = MongoDBQuery.mostSpecificQuery(left.from, right.from)
        if (mostSpec.isDefined) {

            val leftVarsNonShared = left.getVariables diff right.getVariables
            val rightVarsNonShared = right.getVariables diff left.getVariables
            if (leftVarsNonShared.nonEmpty || rightVarsNonShared.nonEmpty)
                return None
            
            val sharedVars = left.getVariables intersect right.getVariables
            if (sharedVars.nonEmpty) {
                // Check if shared variables are projected from the same reference
                sharedVars.foreach(x => {
                    // Get the references corresponding to variable ?x in each query.
                    val leftRefs = left.project.filter(_.as.get == x).map(_.references)
                    val rightRefs = right.project.filter(_.as.get == x).map(_.references)

                    // Verify that at least one projection of ?x is the same in the left and right queries
                    val inter = leftRefs.intersect(rightRefs)
                    if (inter.isEmpty) {
                        if (logger.isTraceEnabled)
                            logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right +
                                "\nbecause of variable " + x + ": left reference " + leftRefs +
                                " do not match right references " + rightRefs)
                        return None
                    }
                })

                /*
                // Check if the same reference is projected as different non-shared variables in both queries
                val leftVarsNonShared = left.getVariables diff right.getVariables
                leftVarsNonShared.foreach(x => {
                    // Get the references corresponding to non-shared variable ?x in the left query
                    val leftRefs = left.project.filter(_.as.get == x).map(_.references)

                    leftRefs.foreach(leftRef => {
                        // Check if there exists the same reference in the right query that is is necessarily with a variable;
                        // this is necessarily a different variable since we check only non-shared variables here.
                        val rightRef = right.project.filter(p => p.references == leftRef && p.as.isDefined && !sharedVars.contains(p.as.get))
                        // If yes, then we have a shared-reference associated to two different variables on the left and right 
                        if (rightRef.nonEmpty) {
                            if (logger.isTraceEnabled)
                                logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right +
                                    "\nbecause reference " + leftRef + " is associated to two different variables " +
                                    x + " and " + rightRef.map(_.as.get))
                            return None
                        }
                    })
                })

                // Check if the same reference is projected as different non-shared variables in both queries
                val rightVarsNonShared = right.getVariables diff left.getVariables
                rightVarsNonShared.foreach(x => {
                    // Get the references corresponding to non-shared variable ?x in the right query
                    val rightRefs = right.project.filter(_.as.get == x).map(_.references)

                    rightRefs.foreach(rightRef => {
                        // Check if there exists the same reference in the left query that is is necessarily with a variable;
                        // this is necessarily a different variable since we check only non-shared variables here.
                        val leftRef = left.project.filter(p => p.references == rightRef && p.as.isDefined && !sharedVars.contains(p.as.get))
                        // If yes, then we have a shared-reference associated to two different variables on the left and right 
                        if (leftRef.nonEmpty) {
                            if (logger.isTraceEnabled)
                                logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right +
                                    "\nbecause reference " + leftRef + " is associated to two different variables " +
                                    x + " and " + leftRef.map(_.as.get))
                            return None
                        }
                    })
                })
             	*/
                val mergedBindings = left.tpBindings ++ right.tpBindings
                val mergedProj = left.project ++ right.project
                val mergedWhere = left.where ++ right.where
                result = Some(new AbstractAtomicQuery(mergedBindings, mostSpec.get, mergedProj, mergedWhere))
            } else if (logger.isTraceEnabled)
                logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right + "\nbecause they have no shared variable.")
        } else if (logger.isTraceEnabled)
            logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right + "\nbecause they have different From parts.")
        result
    }

    /**
     * Try to narrow down this atomic query by propagating conditions of the another atomic query
     * that is joined with it (either inner join or a left join).
     * Below we denote by left query 'this' query, and the other query by right query.
     *
     * Two possibilities are evaluated:<br>
     * (i) Check if the From part of the right query is more specific than From part of the left query.
     * In case of an inner join of left outer join of left and right, the results will be produced from the common
     * documents from left and right, thus we can simply replace the From of the left query with that of the right query.<br>
     * @example
     * <code>left.from = db.collection.find({query1})</code>
     * <code>right.from = db.collection.find({query1, query2})</code>
     * In that case, since we join the two queries, we can replace left.from with right.from that is the most specific.
     *
     * (ii) If the two queries have some shared variables, then Equality and IsNotNull conditions of the right query
     * on those shared variables can be merged with the conditions of the left query.<br>
     * @example
     * Assume we have "$.field1 AS ?x" in left and "$.field2 AS ?x" in right.
     * If the right query has a Where condition <code>Equals($.field2, "value")</code>, then we can add a new condition
     * to the Where conditions of the left query: <code>Equals($.field1, "value")</code>
     *
     * @return an optimized version of 'this', based on the query in parameter, or 'this' is no optimization was possible.
     */
    def propagateConditionFromJoinedQuery(q: AbstractQuery): AbstractAtomicQuery = {

        if (!q.isInstanceOf[AbstractAtomicQuery])
            return this

        val left = this
        val right = q.asInstanceOf[AbstractAtomicQuery]
        var result: Option[AbstractAtomicQuery] = None

        // Check if right From part is more specific than left From part
        var newLeft =
            if (MongoDBQuery.isLeftMoreSpecific(right.from, left.from)) {
                // Narrow down left query by replacing its From part with that of the right query
                new AbstractAtomicQuery(left.tpBindings, right.from, left.project, left.where)
            } else left

        // Check if some Where conditions of the right query can be added to the Where conditions of the left query to narrow it down
        var condsToReport = Set[AbstractQueryCondition]()
        val sharedVars = left.getVariables intersect right.getVariables
        if (sharedVars.nonEmpty) {
            sharedVars.foreach(x => {
                // Get all the references associated with variable ?x in the left and right queries.
                // Note: ?x may be in several projections, e.g. Set($.field1 AS ?x, $.field2 AS ?x),
                // and each projection may contain several references e.g. "($.field11, $.field2) as ?x" for template term maps.
                // Restriction: we deal only with single reference i.e. "ref AS ?x", but not references such as "(ref1, ref2) AS ?x"
                val leftRefs = left.project.filter(p => p.as.isDefined && p.as.get == x && p.references.size == 1).map(_.references.head)
                val rightRefs = right.project.filter(p => p.as.isDefined && p.as.get == x && p.references.size == 1).map(_.references.head)

                rightRefs.foreach(ref => {
                    // Select the Where conditions in the right query, of type isNotNull or Equals, 
                    // that refer to the right reference ref associated with ?x
                    val rightConds = right.where.filter(w => w.hasReference && w.asInstanceOf[IReference].reference == ref)
                    rightConds.foreach(rightCond => {
                        rightCond match {
                            case eq: AbstractQueryConditionEquals => {
                                leftRefs.foreach(leftRef => {
                                    condsToReport += new AbstractQueryConditionEquals(leftRef, eq.eqValue)
                                })
                            }
                            case nn: AbstractQueryConditionNotNull => {
                                leftRefs.foreach(leftRef => {
                                    condsToReport += new AbstractQueryConditionNotNull(leftRef)
                                })
                            }
                        }
                    })
                })
            })
        }

        if (condsToReport.nonEmpty)
            newLeft = new AbstractAtomicQuery(newLeft.tpBindings, newLeft.from, newLeft.project, newLeft.where ++ condsToReport)

        if (logger.isTraceEnabled)
            if (newLeft != this)
                logger.trace("Propagated condition from \n" + right + "\nto\n" + left + "\n producing new optimized query:\n" + newLeft)
        newLeft
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
    def mergeForUnion(q: AbstractQuery): Option[AbstractAtomicQuery] = {

        if (!q.isInstanceOf[AbstractAtomicQuery])
            return None

        val right = q.asInstanceOf[AbstractAtomicQuery]
        var result: Option[AbstractAtomicQuery] = None
        val left = this

        if (left.from == right.from) {
            val mergedBindings = left.tpBindings ++ right.tpBindings
            val mergedProj = left.project ++ right.project
            val leftOr = if (left.where.size > 1) new AbstractQueryConditionAnd(left.where.toList) else left.where.head
            val rightOr = if (right.where.size > 1) new AbstractQueryConditionAnd(right.where.toList) else right.where.head
            val mergedWhere = new AbstractQueryConditionOr(List(leftOr, rightOr))
            result = Some(new AbstractAtomicQuery(mergedBindings, left.from, mergedProj, Set(mergedWhere)))
        }
        result
    }
}
