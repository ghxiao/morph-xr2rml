package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.apache.log4j.Logger
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.sparql.algebra.op.OpProject
import es.upm.fi.dia.oeg.morph.base.AbstractQuery
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GenericQuery
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.querytranslator.ConditionType
import es.upm.fi.dia.oeg.morph.base.querytranslator.IQueryCondition
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryCondition
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import fr.unice.i3s.morph.xr2rml.mongo.MongoDBQuery
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCond
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeAnd
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeUnion

/**
 * Translation of a SPARQL query into a set of MongoDB queries.
 *
 * This class assumes that the xR2RML mapping is normalized, that is, a triples map
 * has not more that one predicate-object map, and each predicate-object map has
 * exactly one predicate and one object map.
 * In the code this assumption is mentioned by the annotation @NORMALIZED_ASSUMPTION
 *
 * @TODO The management of logical source iterators is approximate, must be studied in details.
 */
class MorphMongoQueryTranslator(val md: R2RMLMappingDocument) extends MorphBaseQueryTranslator {

    override val logger = Logger.getLogger(this.getClass());

    /**
     * High level entry point to the query translation process.
     * @TODO only the first query of the AbstractQuery result is managed for the time being
     * @TODO the transTP does not generate an atomic abstract query for the parent triples map if any
     *
     * @return AbstractQuery instance containing a set of concrete queries.
     *
     */
    override def translate(op: Op): AbstractQuery = {
        if (logger.isDebugEnabled()) logger.debug("opSparqlQuery = " + op)

        // WARNING ####################################################################
        // This is totally adhoc code meant to test the whole process of running Morph-xR2RML with
        // a query of one triple pattern. Bindings with triples maps are hard coded here
        val tmMovies = md.getClassMappingsByName("Movies")
        val tmDirectors = md.getClassMappingsByName("Directors")

        // Do the translation of the first triple pattern
        val bgp = op.asInstanceOf[OpProject].getSubOp().asInstanceOf[OpBGP]
        val triples = bgp.getPattern().getList()
        this.transTP(triples.get(0), tmMovies)
        // ####################################################################
    }

    /**
     * Translation of a triple pattern into a set of concrete queries based on a candidate triples map.
     *
     * @param tp a SPARQL triple pattern
     * @param tm a triples map that has been assessed as a candidate triples map for tp, that is a
     * triples map that should potentially be able to generate triples matching tp
     * @return set of concrete query strings
     */
    def transTP(tp: Triple, tm: R2RMLTriplesMap): AbstractQuery = {

        // Sanity checks about the @NORMALIZED_ASSUMPTION
        val poms = tm.getPropertyMappings
        if (poms.isEmpty || poms.size > 1) {
            logger.error("The candidate triples map " + tm.toString + " must have exactly one predicate-object map.")
            return AbstractQuery()
        }

        val pom = poms.head
        if (pom.predicateMaps.size != 1 ||
            !((pom.objectMaps.size == 0 && pom.refObjectMaps.size == 1) || (pom.objectMaps.size == 1 && pom.refObjectMaps.size == 0))) {
            logger.error("The candidate triples map " + tm.toString + " must have exactly one predicate map and one object map.")
            return AbstractQuery()
        }

        val logSrc = tm.getLogicalSource
        if (logSrc.logicalTableType != Constants.LogicalTableType.QUERY) {
            logger.error("Logical source table type is not compatible with MongoDB.")
            return AbstractQuery()
        }

        if (logger.isDebugEnabled()) logger.debug("Translation of triple pattern: [" + tp + "] with triples map " + tm)

        // Start translation
        val fromPart = genFrom(tm)
        val selectPart = genProjection(tp, tm)
        val wherePart = genCond(tp, tm)
        if (logger.isDebugEnabled())
            logger.debug("transTP(" + tp + ", " + tm + "):\n" +
                "fromPart:  " + fromPart + "\n" +
                "selecPart: " + selectPart + "\n" +
                "wherePart: " + wherePart)

        // Select only isNotNull and Equality conditions
        val pushDownConds = wherePart.
            filter(c => c.condType == ConditionType.IsNotNull || c.condType == ConditionType.Equals).
            map(c => c.asInstanceOf[MorphBaseQueryCondition])

        // Generate one abstract MongoDB query for each condition
        val absQueries: List[MongoQueryNode] = pushDownConds.map(cond => {
            // If there is an iterator, replace the heading "$" of the JSONPath reference with the iterator path
            val iter = fromPart.iterator
            val reference =
                if (iter.isDefined) cond.reference.replace("$", iter.get)
                else cond.reference

            // Run the translation into an abstract MongoDB query
            cond.condType match {
                case ConditionType.IsNotNull =>
                    JsonPathToMongoTranslator.trans(reference, new MongoQueryNodeCond(ConditionType.IsNotNull, null))
                case ConditionType.Equals =>
                    JsonPathToMongoTranslator.trans(reference, new MongoQueryNodeCond(ConditionType.Equals, cond.eqValue))
                case _ => throw new MorphException("Unsupported condition type " + cond.condType)
            }
        })

        if (logger.isTraceEnabled())
            logger.trace("Conditions translated to abstract MongoDB query:\n" + absQueries)

            // Create the concrete queries from the set of abstract MongoDB queries
        val queries = toConcreteQueries(fromPart, absQueries).map(q => new GenericQuery(Constants.DatabaseType.MongoDB, q, Some(tm)))

        if (logger.isDebugEnabled())
            logger.debug("transTP(" + tp + ", " + tm + "):\n" + queries)
        AbstractQuery(queries)
    }

    /**
     * Generate the data source from the triples map
     *
     * @param tm a triples map that has been assessed to be a candidate triples map for the translation of tp into a query
     * @return a MongoDBQuery representing the query string
     */
    def genFrom(tm: R2RMLTriplesMap): MongoDBQuery = {
        val logSrc = tm.getLogicalSource
        val query = MongoDBQuery.parseQueryString(logSrc.getValue, logSrc.docIterator, true)
        query
    }

    /**
     * Generate the data source for the parent triples map of the triples map passed
     *
     * @param tm a triples map that has been assessed as a candidate triples map for the translation of tp into a query
     * @return a MongoDBQuery representing the query string of the parent triples map
     */
    def genFromParent(tm: R2RMLTriplesMap): MongoDBQuery = {
        val pom = tm.getPropertyMappings.head // @NORMALIZED_ASSUMPTION

        if (pom.hasRefObjectMap) {
            val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION
            val parentTMLogSrc = md.getParentTriplesMap(rom).getLogicalSource
            val query = MongoDBQuery.parseQueryString(parentTMLogSrc.getValue, parentTMLogSrc.docIterator, true)
            query
        } else
            throw new MorphException("Triples map " + tm + " has no parent triples map")
    }

    /**
     * Generate the list of xR2RML references that are evaluated when generating the triples that match tp.
     * Those references are used to select the data elements to mention in the projection part of the query.
     *
     * @param tp a SPARQL triple pattern
     * @param tm a triples map that has been assessed as a candidate triples map for the translation of tp into a query
     * @return list of references (JSONPath expressions for MongoDB) from which fields must be projected
     */
    def genProjection(tp: Triple, tm: R2RMLTriplesMap): List[String] = {

        var listRefs: List[String] = List.empty

        if (tp.getSubject().isVariable())
            listRefs = tm.subjectMap.getReferences

        val pom = tm.getPropertyMappings.head // @NORMALIZED_ASSUMPTION
        if (tp.getPredicate().isVariable())
            listRefs = listRefs ::: pom.predicateMaps.head.getReferences // @NORMALIZED_ASSUMPTION

        if (pom.hasRefObjectMap) {
            // The joined fields must always be projected, whether tp.obj is an IRI or a variable: since MongoDB
            // cannot compute joins, the xR2RML processor has to do it, thus joined fields must be returned by both queries.
            val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION
            for (jc <- rom.joinConditions)
                listRefs = listRefs :+ jc.childRef
        } else if (tp.getObject().isVariable())
            listRefs = listRefs ++ pom.objectMaps.head.getReferences // @NORMALIZED_ASSUMPTION

        listRefs
    }

    /**
     * Generate the list of xR2RML references that are evaluated when generating the triples that match tp.
     * Those references are used to select the data elements to mention in the projection part of the query.
     *
     * @param tp a SPARQL triple pattern
     * @param tm a triples map that has been assessed as a candidate triples map for the translation of tp into a query
     * @return list of references (JSONPath expressions for MongoDB) from which fields must be projected
     */
    def genProjectionParent(tp: Triple, tm: R2RMLTriplesMap): List[String] = {

        var listRefs: List[String] = List.empty

        val pom = tm.getPropertyMappings.head // @NORMALIZED_ASSUMPTION
        if (pom.hasRefObjectMap) {
            // The joined fields must always be projected, whether tp.obj is an IRI or a variable: since MongoDB
            // cannot compute joins, the xR2RML processor has to do it, thus joined fields must be returned by both queries.
            val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION
            for (jc <- rom.joinConditions) {
                listRefs = listRefs :+ jc.parentRef
            }

            // In addition, if tp.obj is a variable, the subject of the parent TM must be projected too.
            if (tp.getObject().isVariable()) {
                val parentTM = md.getParentTriplesMap(rom)
                listRefs = listRefs ++ parentTM.subjectMap.getReferences // @NORMALIZED_ASSUMPTION
            }
        } else
            throw new MorphException("Triples map " + tm + " has no parent triples map")
        listRefs
    }

    /**
     * Generate the set of conditions to apply to the database query (the from part), so that triples map TM
     * generates triples that match tp.
     * If a triple pattern term is constant (IRI or literal), genCond generates an equality condition,
     * only if the term map is reference-valued or template-valued. Indeed, if the term map is constant-valued and
     * the tp term is constant too, we should already have checked that they were compatible in the bind_m function.
     *
     * If a triple pattern term is variable, genCond generates a not-null condition, only
     * if the term map is reference-valued or template-valued. Indeed, if the term map is constant-valued, then the variable
     * will simply be bound to the constant value of the term map but that does not entail any condition in the query.
     *
     * A condition is either isNotNull(JSONPath expression) or equals(JSONPath expression, value)
     *
     * @param tp a SPARQL triple pattern
     * @param tm a triples map that has been assessed as a candidate triples map for the translation of tp into a query
     * @return set of conditions, either non-null or equality
     */
    def genCond(tp: Triple, tm: R2RMLTriplesMap): List[IQueryCondition] = {

        var conditions: List[IQueryCondition] = List.empty

        { // === Subject map ===
            val tpTerm = tp.getSubject
            val termMap = tm.subjectMap

            if (termMap.isReferenceOrTemplateValued)
                if (tpTerm.isVariable)
                    // Add a not-null condition for each reference in the term map
                    conditions = conditions ++ termMap.getReferences.map(ref => MorphBaseQueryCondition.notNull(ref))
                else if (tpTerm.isURI) {
                    // Add an equality condition for each reference in the term map
                    conditions = conditions ++ genEqualityConditions(termMap, tpTerm)
                }
        }
        { // === Predicate map ===
            val tpTerm = tp.getPredicate
            val termMap = tm.predicateObjectMaps.head.predicateMaps.head // @NORMALIZED_ASSUMPTION

            if (termMap.isReferenceOrTemplateValued)
                if (tpTerm.isVariable)
                    // Add a not-null condition for each reference in the term map
                    conditions = conditions ++ termMap.getReferences.map(ref => MorphBaseQueryCondition.notNull(ref))
                else if (tpTerm.isURI)
                    // Add an equality condition for each reference in the term map
                    conditions = conditions ++ genEqualityConditions(termMap, tpTerm)
        }
        { // === Object map ===
            val tpTerm = tp.getObject
            val pom = tm.predicateObjectMaps.head // @NORMALIZED_ASSUMPTION

            if (tpTerm.isLiteral) {
                // Add an equality condition for each reference in the term map
                val termMap = pom.objectMaps.head // @NORMALIZED_ASSUMPTION
                if (termMap.isReferenceOrTemplateValued)
                    conditions = conditions ++ genEqualityConditions(termMap, tpTerm)

            } else if (tpTerm.isURI) {

                if (pom.hasRefObjectMap) {
                    val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION
                    // Add non-null condition on the child reference 
                    for (jc <- rom.joinConditions)
                        conditions = conditions :+ MorphBaseQueryCondition.notNull(jc.childRef)
                } else {
                    // tp.obj is a constant IRI and there is no RefObjectMap: add an equality condition for each reference in the term map
                    val termMap = pom.objectMaps.head // @NORMALIZED_ASSUMPTION
                    if (termMap.isReferenceOrTemplateValued)
                        conditions = conditions ++ genEqualityConditions(termMap, tpTerm)
                }

            } else if (tpTerm.isVariable) {

                if (pom.hasRefObjectMap) {
                    val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION
                    // Add non-null condition on the child reference 
                    for (jc <- rom.joinConditions)
                        conditions = conditions :+ MorphBaseQueryCondition.notNull(jc.childRef)
                } else {
                    // tp.obj is a Variable and there is no RefObjectMap: add a non-null condition for each reference in the term map
                    val termMap = pom.objectMaps.head // @NORMALIZED_ASSUMPTION
                    if (termMap.isReferenceOrTemplateValued)
                        conditions = conditions ++ termMap.getReferences.map(ref => MorphBaseQueryCondition.notNull(ref))
                }
            }
        }

        if (logger.isDebugEnabled()) logger.debug("Translation returns conditions: " + conditions)
        conditions
    }

    /**
     *
     * Generate the set of conditions to match the object of a triple pattern with he subject
     * of a referencing object map.
     * A condition is either isNotNull(JSONPath expression) or equals(JSONPath expression, value)
     *
     * @param tp a SPARQL triple pattern
     * @param tm a triples map that has been assessed as a candidate triples map for the translation of tp into a query
     * @return set of conditions, either non-null or equality
     */
    def genCondParent(tp: Triple, tm: R2RMLTriplesMap): List[IQueryCondition] = {

        var conditions: List[IQueryCondition] = List.empty

        { // === Object map ===
            val tpTerm = tp.getObject
            val pom = tm.predicateObjectMaps.head // @NORMALIZED_ASSUMPTION

            if (!pom.hasRefObjectMap)
                throw new MorphException("Triples map " + tm + " has no parent triples map")

            if (tpTerm.isURI) {
                // tp.obj is a constant IRI to be matched with the subject of the parent TM:
                // add an equality condition for each reference in the subject map of the parent TM
                val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION
                val parentSM = md.getParentTriplesMap(rom).subjectMap
                if (parentSM.isReferenceOrTemplateValued)
                    conditions = conditions ++ genEqualityConditions(parentSM, tpTerm)

                // Add non-null condition on the parent reference
                for (jc <- rom.joinConditions)
                    conditions = conditions :+ MorphBaseQueryCondition.notNull(jc.parentRef)

            } else if (tpTerm.isVariable) {
                // tp.obj is a SPARQL variable to be matched with the subject of the parent TM
                val rom = pom.getRefObjectMap(0) // @NORMALIZED_ASSUMPTION
                val parentSM = md.getParentTriplesMap(rom).subjectMap
                if (parentSM.isReferenceOrTemplateValued)
                    conditions = conditions ++ parentSM.getReferences.map(ref => MorphBaseQueryCondition.notNull(ref))

                // Add non-null condition on the parent reference
                for (jc <- rom.joinConditions)
                    conditions = conditions :+ MorphBaseQueryCondition.notNull(jc.parentRef)
            }
        }

        if (logger.isDebugEnabled()) logger.debug("Translation returns Parent conditions: " + conditions)
        conditions
    }

    /**
     * Generate one equality condition between a reference in the term map (Column name, JSONPath expression...)
     * and a term of a triple pattern.<br>
     * If the term map is reference-valued, one equality condition is generated.<br>
     * If the term map is template-value, possibly several equality conditions are generated, i.e. one for each
     * capturing group in the template string.<br>     *
     * Return an empty list for other types of term map.
     *
     * @param targetQuery child or parent, i.e. the query to which references of termMap relate to
     * @param termMap the term map that should possibly generate terms that match the triple pattern term tpTerm
     * @param tpTerm a term from a triple pattern
     * @return a list of one equality condition for a reference-value term map, possibly several conditions
     * for a template-valued term map
     */
    private def genEqualityConditions(termMap: R2RMLTermMap, tpTerm: Node): List[MorphBaseQueryCondition] = {

        if (termMap.isReferenceValued) {
            // Make a new equality condition between the reference in the term map and the value in the triple pattern term
            List(MorphBaseQueryCondition.equality(termMap.getOriginalValue, tpTerm.toString(false)))

        } else if (termMap.isTemplateValued) {
            // Get the references of the template string and the associated values in the triple pattern term
            val refValueCouples = TemplateUtility.getTemplateMatching(termMap.getOriginalValue, tpTerm.toString(false))

            // For each reference and associated value, build a new equality condition
            val refValueConds = refValueCouples.map(m => MorphBaseQueryCondition.equality(m._1, m._2))
            refValueConds.toList
        } else
            List.empty
    }

    /**
     * Translate a set of non-optimized MongoQueryNode instances into one or more optimized concrete MongoDB queries.
     *
     * All MongoQueryNodes are grouped under a top-level AND query, unless there is only one condition.
     * Then the query is optimized, which may come up with a top-level UNION.
     *
     * A query string is generated, that contains the initial query string from the logical source.
     * A UNION is translated into several concrete queries, for any other type of query only one query string is returned.
     *
     * @param fromPart the query of the logical source
     * @param absQueries the actual set of NotNull or Equality conditions to translate into concrete MongoDB queries.
     * @return list of MongoDBQuery instances
     */
    def toConcreteQueries(
        fromPart: MongoDBQuery,
        absQueries: List[MongoQueryNode]): List[MongoDBQuery] = {

        // If there are more than 1 query, encapsulate them under a top-level AND and optimize the resulting query
        var Q =
            if (absQueries.length == 1) absQueries(0).optimize
            else new MongoQueryNodeAnd(absQueries).optimize
        if (logger.isTraceEnabled())
            logger.trace("Condtion set was translated into: " + Q)

        // @TODO If the query is an AND, merge its members that have a common root JSON field
        // Example 1. AND('a':{$gt:10}, 'a':{$lt:20}) => 'a':{$gt:10, $lt:20}, which is not possible so far
        // due to the way the MongoQueryNodeField is designed: it can have only one condition.
        // Example 2. AND('a.b':{$elemMatch:{Q1}}, 'a.b':{$elemMatch:{Q2}}) => 'a.b': {$elemMatch:{$and:[{Q1},{Q2}]}}.
        // Need to continue function MongoQueryNode.fusionQueries()

        val Qstr: List[MongoDBQuery] =
            if (Q.isUnion)
                // For each member of the UNION, convert it to a query string, add the query from the logical source,
                // and create a MongoDBQuery instance
                Q.asInstanceOf[MongoQueryNodeUnion].members.map(
                    q => new MongoDBQuery(
                        fromPart.collection,
                        q.toTopLevelQuery(fromPart.query),
                        q.projection.map(o => o.toString)))
            else
                // If no UNION, convert to a query string, add the query from the logical source, and create a MongoDBQuery instance
                List(new MongoDBQuery(fromPart.collection, Q.toTopLevelQuery(fromPart.query), Q.projection.map(o => o.toString)))
                
        if (logger.isTraceEnabled())
            logger.trace("Final set of concrete queries: [" + Qstr + "]")
        Qstr
    }
}
