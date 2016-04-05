package fr.unice.i3s.morph.xr2rml.mongo.abstractquery

import org.apache.log4j.Logger

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultRdfTerms
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.AbstractQuery
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryCondition
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionAnd
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionEquals
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionIsNull
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionNotNull
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionOr
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryProjection
import es.upm.fi.dia.oeg.morph.base.query.ConditionType
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery
import es.upm.fi.dia.oeg.morph.base.query.IReference
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryOptimizer
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.TPBinding
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
 * term map, e.g. if ?x is matched with "http://domain/{\$.ref1}/{\$.ref2.*}", then the projection is:
 * "Set(\$.ref1, \$.ref2.*) AS ?x" => in that case, we must have the same projection in the second query
 * for the merge to be possible.<br>
 * Therefore projection is a set of sets, in the most complex case we may have something like:
 * Set(Set(\$.ref1, \$.ref2.*) AS ?x, Set(\$.ref3, \$.ref4) AS ?x), Set(\$.ref5, \$.ref6) AS ?x))
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
     * Translate an atomic abstract query into one or several concrete queries whose results must be UNIONed:<br>
     * 1. the Where part of the atomic abstract query is translated into an abstract
     * MongoDB query using function JsonPathToMongoTranslator.trans().<br>
     * 2. the abstract MongoDB query is optimized and translated into a set of concrete MongoDB queries.<br>
     *
     * The result is stored in attribute 'this.targetQuery'.
     *
     * @param translator the query translator
     */
    override def translateAtomicAbstactQueriesToConcrete(translator: MorphBaseQueryTranslator): Unit = {
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
     * Get the xR2RML projection of variable ?x in this query.
     *
     * @note When a variable ?x is matched with a term map with template string
     * "http://domain/{\$.ref1}/{\$.ref2.*}", then we say that
     * Set($.ref1, $.ref2) is projected as ?x.<br>
     * In addition a variable can be projected several times in the same query, when
     * it appears several times in a triple pattern like "?x :prop ?x".<br>
     * Therefore the result is a Set of projections of x, each containing a Set of references
     *
     * @param varName the variable name
     * @return a set of projections in which the 'as' field is defined and equals 'varName'
     */
    private def getProjectionsForVariable(varName: String): Set[AbstractQueryProjection] = {
        this.project.filter(p => { p.as.isDefined && p.as.get == varName })
    }

    /**
     * Get the variables projected in this query with a given projection
     *
     * @param proj the projection given as a set of xR2RML references (this is the 'references'
     * field of an instance of AbstractQueryProjection)
     * @return a set of variable names projected as 'proj'
     */
    private def getVariablesForProjection(proj: Set[String]): Set[String] = {
        this.project.filter(p => { p.references == proj && p.as.isDefined }).map(_.as.get)
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
            logger.info("Generating RDF terms under binding " + tpb + " for atomic query: \n" + this.toStringConcrete);

            // Execute the queries of tagetQuery
            var start = System.currentTimeMillis()
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
            var end = System.currentTimeMillis()
            logger.info("Atomic query returned " + resultSet.size + " results in " + (end - start) + "ms.")

            // Main loop: iterate and process each result document of the result set
            start = System.currentTimeMillis()
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
            end = System.currentTimeMillis()
            logger.info("Atomic query generated " + result.size + " RDF triples in " + (end - start) + "ms.")
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
     * Two cases are considered:
     * 
     * (1) The two queries have the same From part or one is a sub-set of the other, and they are 
     * joined on a at least one variable whose reference is declared as unique in the triples map 
     * logical source (xrr:UniqueRef).
     * Example:<br>
     * q1.from: <code>db.collection.find({query1})</code>,<br>
     * q2.from: <code>db.collection.find({query1, query2})</code><br>
     * q2.from is a sub-set of q1.from, in other words q2.from is more specific than q1.from.<br>
     * If q1.project and q2.project contain the same projection "$.field AS ?x", and both
     * logical sources declare property <code>xrr:uniqueRef "$.field"</code>, then this is a self-join.
     * 
     * (2) When no unique reference is involved, the merge is allowed if and only if the conditions 
     * below are met:<br>
     * (i) both queries have the same From part (the logical source) and Where part;<br>
     * (ii) they have at least one shared variable (on which the join is to be done) and
     * the shared variables are projected from the same xR2RML reference(s) in both queries (see example 1);<br>
     * (iv) if there are non-shared variables then this is not a self-join (see example 2).
     * However, if each non-shared variable is projected from an xR2RML reference that is also projected
     * as a shared variable (4th example), then this is a proper self-join.<br>
     * Note: even if all non-shared variable belong to the same query then there are cases of
     * query that are not a self-join (see example 3).
     *
     * <b>Example 1</b>: if Q1 and Q2 have the same logical source and Where conditions, and they have a shared variable ?x,
     * if both queries project the same reference as ?x: "$.field AS ?x", then this is a self-join.<br>
     * On the contrary, if projections are different: "$.field1 AS ?x" in Q1 and "$.field2 AS ?x" is Q2,
     * then this is not a self-join, on the contrary this is a regular join.<br>
     *
     * <b>Example 2</b>:  In a graph pattern like:
     * <code>?x prop ?y. ?x prop ?z. FILTER (?y != ?z)</code>,<br>
     * if Q1 and Q2 with the same logical source and Where part, and<br>
     * Q1 has projections "$.x AS ?x", "$.a AS ?y",<br>
     * Q2 has projections "$.x AS ?x", "$.b AS ?z".<br>
     * then this is not a self-join because there are non-shared variables whose reference
     * is not the reference of a shared variable (as in example 4).<br>
     *
     * <b>Example 3</b>: In a graph pattern like:
     * <code>?x prop ex:blabla. ?x prop ?z. FILTER (?z != ex:blabla)</code>,<br>
     * if Q1 and Q2 with the same logical source and Where part, and<br>
     * Q1 has projections "$.a AS ?x"<br>
     * Q2 has projections "$.a AS ?x", "$.b AS ?z"<br>
     * then this is not a self-join, as illustrated by the filter.<br>
     *
     * <b>Example 4</b>: If Q1 and Q2 have the same logical source, and<br>
     * Q1 has projections "$.a AS ?x", "$.a AS ?y",<br>
     * Q2 has projections "$.a AS ?x", "$.a AS ?z"<br>
     * then this is a proper self-join because non-shared variables ?y and ?z are projected from
     * reference "$.a" that happens to be projected as the shared variable ?x.
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
    def mergeForInnerJoin(q: AbstractQuery): Option[AbstractAtomicQuery] = {

        if (!q.isInstanceOf[AbstractAtomicQuery])
            return None

        val left = this
        val right = q.asInstanceOf[AbstractAtomicQuery]

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
                        if (logger.isTraceEnabled)
                            logger.trace("Detected join on unique reference(s) " + joinedUniqueRefs)
                        isJoinOnUniqueRefs = true
                    }
                }
            })
        }

        if (isJoinOnUniqueRefs) {
            // ----------------------------- Most favorable case: join on a variable bound to a unique reference ---------------------------
            val mergedBindings = left.tpBindings ++ right.tpBindings
            val mergedProj = left.project ++ right.project
            val mergedWhere = left.where ++ right.where
            val result = Some(new AbstractAtomicQuery(mergedBindings, mostSpec.get, mergedProj, mergedWhere))
            result
        } else {

            // ----------------------------- Standard Case: no join on a unique reference ---------------------------

            // If both queries do NOT have the same From part or they are NOT joined on a unique reference,
            // then the merge is possible only if they have exactly the same From and Where parts
            if (!MongoDBQuery.sameQueries(left.from, right.from)) {
                if (logger.isTraceEnabled)
                    logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right + "\nbecause they have different From parts.")
                return None
            }

            if (left.where != right.where) {
                if (logger.isTraceEnabled)
                    logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right + "\nbecause they have different Where parts.")
                return None
            }

            // If the two queries have no shared variable then this cannot be a self-join
            if (sharedVars.isEmpty) {
                if (logger.isTraceEnabled)
                    logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right + "\nbecause they have no shared variable.")
                None
            }

            // If there are non-shared variables then this is not a self-join (see 2nd and 3rd examples),
            // unless each non-shared variable is projected from an xR2RML reference that is also projected
            // as a shared variable (4th example), then this is a proper self-join.

            val nonSharedVarsLeft = left.getVariables diff right.getVariables
            nonSharedVarsLeft.foreach(x => {
                // Get the projections associated with the non-shared variable x in the left query
                val projs = left.getProjectionsForVariable(x)

                // If one of these projections is not a projection of a shared variable, then this cannot be a self-join
                projs.foreach(proj => {
                    if (!(sharedVarsLeftProjections contains proj.references)) {
                        if (logger.isTraceEnabled)
                            logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right + "\nbecause they have a non-shared variable " + x)
                        return None
                    }
                })
            })

            val nonSharedVarsRight = right.getVariables diff left.getVariables
            nonSharedVarsRight.foreach(x => {
                // Get the projections associated with the non-shared variable x in the right query
                val projs = right.getProjectionsForVariable(x)

                // If one of these projections is not a projection of a shared variable, then this cannot be a self-join
                projs.foreach(proj => {
                    if (!(sharedVarsRightProjections contains proj.references)) {
                        if (logger.isTraceEnabled)
                            logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right + "\nbecause they have a non-shared variable " + x)
                        return None
                    }
                })
            })

            // Check if the shared variables are projected from the same reference
            sharedVars.foreach(x => {
                // Get the references corresponding to variable ?x in each query.
                val leftRefs = left.getProjectionsForVariable(x).map(_.references)
                val rightRefs = right.getProjectionsForVariable(x).map(_.references)

                // Verify that at least one projection of ?x is the same in the left and right queries
                if (leftRefs intersect rightRefs isEmpty) {
                    if (logger.isTraceEnabled)
                        logger.trace("Self-join elimination impossible bewteen \n" + left + "\nand\n" + right +
                            "\nbecause shared variable " + x + " is projected from left references " + leftRefs +
                            " that do not match right references " + rightRefs)
                    return None
                }
            })

            // Build the result query. Both queries have the same From and Where, we can use any of them, here the left ones
            val mergedBindings = left.tpBindings ++ right.tpBindings
            val mergedProj = left.project ++ right.project
            val result = Some(new AbstractAtomicQuery(mergedBindings, left.from, mergedProj, left.where))
            result
        }
    }

    /**
     * Merge this atomic abstract query with another one in order to perform self-join elimination
     * in the context of a referencing object map: join between queries of child and parent triples maps.
     * Two cases are considered:
     * 
     * (1) The two queries have the same From part or one is a sub-set of the other, and they are 
     * joined on a reference declared as unique in the triples map logical source (xrr:UniqueRef). 
     * Example:<br>
     * q1.from: <code>db.collection.find({query1})</code>,<br>
     * q2.from: <code>db.collection.find({query1, query2})</code><br>
     * q2.from is a sub-set of q1.from, i.e. q2.from is more specific than q1.from.<br>
     * If the child and parent references are both "$.field", and both logical sources declare property 
     * <code>xrr:uniqueRef "$.field"</code>, then this is a self-join.
     * 
     * (2) When no unique reference is involved, the merge is allowed if and only if both have 
     * the same From part (the logical source) and Where part.
     *
     * @note This is a simplified version of method mergeForInnerJoin().
     *
     * @param childRef the xR2RML child reference of the join condition: rr:joinCondition [ ... rr:child ... ]
     * @param parent the atomic query representing the parent triples map
     * @param parentRef the xR2RML parent reference of the join condition: rr:joinCondition [ ... rr:parent ... ]
     * @return an AbstractAtomicQuery if the merge is possible, None otherwise
     */
    def mergeForInnerJoinRef(childRef: String,
                             parent: AbstractAtomicQuery,
                             parentRef: String): Option[AbstractAtomicQuery] = {

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
            // ----------------------------- Most favorable case: join on a a unique reference ---------------------------
            val mergedBindings = child.tpBindings ++ parent.tpBindings
            val mergedProj = child.project ++ parent.project
            val mergedWhere = child.where ++ parent.where
            val result = Some(new AbstractAtomicQuery(mergedBindings, mostSpec.get, mergedProj, mergedWhere))
            result
        } else {

            // ----------------------------- Standard Case: no join on a unique reference ---------------------------

            // If both queries do NOT have the same From part or they are NOT joined on a unique reference,
            // then the merge is possible only if they have exactly the same From and Where parts
            if (!MongoDBQuery.sameQueries(child.from, parent.from)) {
                if (logger.isTraceEnabled)
                    logger.trace("Self-join elimination impossible bewteen \n" + child + "\nand\n" + parent + "\nbecause they have different From parts.")
                return None
            }

            if (child.where != parent.where) {
                if (logger.isTraceEnabled)
                    logger.trace("Self-join elimination impossible bewteen \n" + child + "\nand\n" + parent + "\nbecause they have different Where parts.")
                return None
            }

            // Build the result query. Both queries have the same From and Where, we can use any of them, here the left ones
            val mergedBindings = child.tpBindings ++ parent.tpBindings
            val mergedProj = child.project ++ parent.project
            val result = Some(new AbstractAtomicQuery(mergedBindings, child.from, mergedProj, child.where))
            result
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
    def propagateConditionFromJoinedQuery(q: AbstractQuery): AbstractAtomicQuery = {

        if (!q.isInstanceOf[AbstractAtomicQuery])
            return this

        val left = this
        val right = q.asInstanceOf[AbstractAtomicQuery]
        val sharedVars = left.getVariables intersect right.getVariables

        // Check if some Where conditions of the right query can be added to the Where conditions of the left query to narrow it down
        var condsToReport = Set[AbstractQueryCondition]()
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

        if (condsToReport.nonEmpty) {
            val newLeft = new AbstractAtomicQuery(left.tpBindings, left.from, left.project, left.where ++ condsToReport)
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
    def mergeForUnion(q: AbstractQuery): Option[AbstractAtomicQuery] = {

        if (!q.isInstanceOf[AbstractAtomicQuery])
            return None

        val right = q.asInstanceOf[AbstractAtomicQuery]
        var result: Option[AbstractAtomicQuery] = None
        val left = this

        if (MongoDBQuery.sameQueries(left.from, right.from)) {
            val mergedBindings = left.tpBindings ++ right.tpBindings
            val mergedProj = left.project ++ right.project
            val leftOr = if (left.where.size > 1) AbstractQueryConditionAnd.create(left.where) else left.where.head
            val rightOr = if (right.where.size > 1) AbstractQueryConditionAnd.create(right.where) else right.where.head
            val mergedWhere = AbstractQueryConditionOr.create(Set(leftOr, rightOr))
            result = Some(new AbstractAtomicQuery(mergedBindings, left.from, mergedProj, Set(mergedWhere)))
        }
        result
    }

    private def equalFromParts(s1: xR2RMLLogicalSource, s2: xR2RMLLogicalSource): Boolean = {
        val q1 = s1.asInstanceOf[xR2RMLQuery]
        val q2 = s2.asInstanceOf[xR2RMLQuery]

        q1.logicalTableType == q2.logicalTableType && q1.refFormulation == q2.refFormulation &&
            q1.docIterator == q2.docIterator && GeneralUtility.cleanString(q1.query) == GeneralUtility.cleanString(q2.query)

        false
    }
}
