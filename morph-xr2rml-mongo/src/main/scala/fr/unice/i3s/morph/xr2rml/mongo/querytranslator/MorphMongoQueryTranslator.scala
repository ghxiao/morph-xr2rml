package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import scala.collection.JavaConversions._

import org.apache.log4j.Logger

import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct
import com.hp.hpl.jena.sparql.algebra.op.OpFilter
import com.hp.hpl.jena.sparql.algebra.op.OpJoin
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin
import com.hp.hpl.jena.sparql.algebra.op.OpOrder
import com.hp.hpl.jena.sparql.algebra.op.OpProject
import com.hp.hpl.jena.sparql.algebra.op.OpSlice
import com.hp.hpl.jena.sparql.algebra.op.OpUnion
import com.hp.hpl.jena.sparql.core.BasicPattern

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQuery
import es.upm.fi.dia.oeg.morph.base.querytranslator.ConditionType
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryProjection
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import fr.unice.i3s.morph.xr2rml.mongo.MongoDBQuery
import fr.unice.i3s.morph.xr2rml.mongo.abstractquery.MorphAbstractAtomicQuery
import fr.unice.i3s.morph.xr2rml.mongo.abstractquery.MorphAbstractQueryInnerJoin
import fr.unice.i3s.morph.xr2rml.mongo.abstractquery.MorphAbstractQueryInnerJoinRef
import fr.unice.i3s.morph.xr2rml.mongo.abstractquery.MorphAbstractQueryInnerJoinRef
import fr.unice.i3s.morph.xr2rml.mongo.abstractquery.MorphAbstractQueryUnion
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeAnd
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCond
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeUnion

/**
 * Translation of a SPARQL query into a set of MongoDB queries.
 *
 * This class assumes that the xR2RML mapping is normalized, that is, a triples map
 * has not more that one predicate-object map, and each predicate-object map has
 * exactly one predicate and one object map.
 * In the code this assumption is mentioned by the annotation @NORMALIZED_ASSUMPTION
 *
 * @author Franck Michel (franck.michel@cnrs.fr)
 */
class MorphMongoQueryTranslator(val md: R2RMLMappingDocument) extends MorphBaseQueryTranslator {

    this.mappingDocument = md

    override val logger = Logger.getLogger(this.getClass());

    /**
     * High level entry point to the query translation process.
     *
     * @TODO Several features are not implemented:
     * - The binding of a triples map to the triple patterns is hard coded.
     * - The project part is calculated but not managed: all fields of MongoDB documents are retrieved.
     * Besides, only the references are listed, but they must be bound to the variable they represent
     * (the AS of the project part) so that the INNER JOIN can be computed, and from the reference
     * we must figure out which field exactly is to be projected and translate this into a MongoDB
     * collection.find() projection parameter. E.g.: $.field.* => {'field':true}
     * - The iterator of the logical source is not taken into account when computing the WHERE part, i.e.
     * the conditions on the JSONPath references from the mapping.
     *
     * @return a MorphAbstractQuery instance in which the targetQuery parameter has been set with
     * a list containing a set of concrete queries.
     */
    override def translate(op: Op): MorphAbstractQuery = {
        if (logger.isDebugEnabled()) logger.debug("opSparqlQuery = " + op)

        // Translate the SPARQL query to abstract queries
        val res = translateSparqlQuery(op)
        // Translate the abstract queries into concrete MongoDB queries
        res.get.translateAtomicAbstactQueriesToConcrete(this)
        res.get
    }

    /**
     * Retrieve the triples maps bound to a triple pattern
     * @TODO The binding of a triples map to the triple patterns is hard coded.
     */
    private def getBoundTriplesMaps(triple: Triple): List[R2RMLTriplesMap] = {
        val tmMovies = md.getClassMappingsByName("Movies")
        val tmDirectors = md.getClassMappingsByName("Directors")
        List(tmDirectors)
    }

    private def translateSparqlQuery(op: Op): Option[MorphAbstractQuery] = {
        op match {
            case opProject: OpProject => { // SELECT clause
                val subOp = opProject.getSubOp();
                this.translateSparqlQuery(subOp)
            }
            case bgp: OpBGP => { // Basic Graph Pattern
                val triples = bgp.getPattern.getList.toList
                if (triples.size == 0)
                    None
                else if (triples.size == 1)
                    Some(this.transTPm(triples.head, this.getBoundTriplesMaps(triples.head)))
                else {
                    // Make an INER JOIN between the first triple pattern and the rest of the triple patterns
                    val left = this.transTPm(triples.head, this.getBoundTriplesMaps(triples.head))
                    val right = this.translateSparqlQuery(new OpBGP(BasicPattern.wrap(triples.tail)))
                    if (right.isDefined)
                        Some(new MorphAbstractQueryInnerJoin(left, right.get))
                    else
                        Some(left)
                }
            }
            case opJoin: OpJoin => { // AND pattern
                val left = translateSparqlQuery(opJoin.getLeft)
                val right = translateSparqlQuery(opJoin.getRight)
                if (left.isDefined && right.isDefined)
                    Some(new MorphAbstractQueryInnerJoin(left.get, right.get))
                else if (left.isDefined)
                    left
                else if (right.isDefined)
                    right
                else None
            }
            case opUnion: OpUnion => { //UNION pattern
                val left = translateSparqlQuery(opUnion.getLeft)
                val right = translateSparqlQuery(opUnion.getRight)

                if (left.isDefined && right.isDefined)
                    Some(new MorphAbstractQueryUnion(List(left.get, right.get)))
                else if (left.isDefined)
                    left
                else if (right.isDefined)
                    right
                else None
            }
            case opLeftJoin: OpLeftJoin => { //OPT pattern
                logger.warn("SPARQL OPTIONAL no supported in query translation.")
                None
            }
            case opFilter: OpFilter => { //FILTER pattern
                logger.warn("SPARQL Filter no supported in query translation.")
                None
            }
            case opSlice: OpSlice => {
                logger.warn("SPARQL Slice no supported in query translation.")
                None
            }
            case opDistinct: OpDistinct => {
                logger.warn("SPARQL DISTINCT no supported in query translation.")
                None
            }
            case opOrder: OpOrder => {
                logger.warn("SPARQL ORDER no supported in query translation.")
                None
            }
            case _ => {
                logger.warn("SPARQL feature no supported in query translation.")
                None
            }
        }
    }

    /**
     * Translation of a triple pattern into an abstract query under a set of xR2RML triples maps
     *
     * @param tp a SPARQL triple pattern
     * @param tmSet a set of triples map that are bound to tp, i.e. they are candidates
     * that may potentially generate triples matching tp
     * @return abstract query. This may be an UNION if there are multiple triples maps,
     * and this may contain INNER JOINs for triples maps that have a referencing object map (parent triples map).
     * If there is only one triples map and no parent triples map, the result is an atomic abstract query.
     * @throws MorphException
     */
    override def transTPm(tp: Triple, tmSet: List[R2RMLTriplesMap]): MorphAbstractQuery = {

        val unionOf = for (tm <- tmSet) yield {

            // Sanity checks about the @NORMALIZED_ASSUMPTION
            val poms = tm.getPropertyMappings
            if (poms.isEmpty || poms.size > 1)
                throw new MorphException("The candidate triples map " + tm.toString + " must have exactly one predicate-object map.")
            val pom = poms.head
            if (pom.predicateMaps.size != 1 ||
                !((pom.objectMaps.size == 0 && pom.refObjectMaps.size == 1) || (pom.objectMaps.size == 1 && pom.refObjectMaps.size == 0)))
                throw new MorphException("The candidate triples map " + tm.toString + " must have exactly one predicate map and one object map.")

            // Start translation
            val from = tm.logicalSource
            val project = genProjection(tp, tm)
            val where = genCond(tp, tm)
            val Q =
                if (!pom.hasRefObjectMap)
                    // If there is no parent triples map, simply return this atomic abstract query
                    new MorphAbstractAtomicQuery(Some(tp), Some(tm), from, project, where)
                else {
                    // If there is a parent triples map, create an INNER JOIN ON childRef = parentRef
                    val q1 = new MorphAbstractAtomicQuery(None, None, from, project, where) // no tp nor TM in case of a RefObjectMap
                    val rom = pom.getRefObjectMap(0)
                    val Pfrom = mappingDocument.getParentTriplesMap(rom).logicalSource
                    val Pproject = genProjectionParent(tp, tm)
                    val Pwhere = genCondParent(tp, tm)
                    val q2 = new MorphAbstractAtomicQuery(None, None, Pfrom, Pproject, Pwhere) // no tp nor TM in case of a RefObjectMap 

                    if (rom.joinConditions.size != 1)
                        logger.warn("Multiple join conditions not supported in a ReferencingObjectMap. Considering only the first one.")
                    val jc = rom.joinConditions.toIterable.head // assume only one join condition 
                    new MorphAbstractQueryInnerJoinRef(tp, Some(tm), q1, jc.childRef, q2, jc.parentRef)
                }
            Q // yield query Q for triples map tm
        }

        // If only one triples map then we return the abstract query for that TM
        val resultQ =
            if (unionOf.size == 1)
                unionOf.head
            else
                // If several triples map, then we return a UNION of the abstract queries for each TM
                new MorphAbstractQueryUnion(unionOf)

        if (logger.isDebugEnabled())
            logger.debug("transTPm: Translation of triple pattern: [" + tp + "] with triples maps " + tmSet + ":\n" + resultQ.toString)
        resultQ
    }

    /**
     * Translate an atomic abstract query into one or several concrete queries whose results must be UNIONed.
     *
     * First, the atomic abstract query is translated into an abstract MongoDB query using the
     * JsonPathToMongoTranslator.trans() function.
     * Then, the abstract MongoDB query is translated into a set of concrete MongoDB queries
     * by function mongoAbstractQuerytoConcrete().
     *
     * @param atomicQ the abstract atomic query
     * @return list of concrete query strings whose results must be UNIONed
     */
    override def atomicAbstractQuerytoConcrete(q: MorphAbstractQuery): List[GenericQuery] = {

        // Select isNotNull and Equality conditions
        val atomicQ = q.asInstanceOf[MorphAbstractAtomicQuery]
        val whereConds = atomicQ.where.filter(c => c.condType == ConditionType.IsNotNull || c.condType == ConditionType.Equals)

        // Generate one abstract MongoDB query for each isNotNull and Equality condition
        val mongAbsQs: List[MongoQueryNode] = whereConds.map(cond => {
            // If there is an iterator, replace the heading "$" of the JSONPath reference with the iterator path
            val iter = atomicQ.from.docIterator
            val reference =
                if (iter.isDefined) cond.reference.replace("$", iter.get)
                else cond.reference

            // Translate the condition on a JSONPath reference into an abstract MongoDB query (a MongoQueryNode)
            cond.condType match {
                case ConditionType.IsNotNull =>
                    JsonPathToMongoTranslator.trans(reference, new MongoQueryNodeCond(ConditionType.IsNotNull, null))
                case ConditionType.Equals =>
                    JsonPathToMongoTranslator.trans(reference, new MongoQueryNodeCond(ConditionType.Equals, cond.eqValue))
                case _ => throw new MorphException("Unsupported condition type " + cond.condType)
            }
        })

        // If there are several queries (more than 1), encapsulate them under a top-level AND
        val mongAbsQ =
            if (mongAbsQs.size > 1) new MongoQueryNodeAnd(mongAbsQs)
            else mongAbsQs(0)
        if (logger.isTraceEnabled())
            logger.trace("Conditions translated to abstract MongoDB query:\n" + mongAbsQ)

        // Create the concrete query/queries from the set of abstract MongoDB queries
        val from = MongoDBQuery.parseQueryString(atomicQ.from.getValue, atomicQ.from.docIterator, true)
        val queries = mongoAbstractQuerytoConcrete(from, atomicQ.project, mongAbsQ)

        // Generate one GenericQuery for each concrete MongoDB query and assign the result as the target query
        queries.map(q => new GenericQuery(Constants.DatabaseType.MongoDB, q, atomicQ.from.docIterator))
    }

    /**
     * Translate a non-optimized MongoQueryNode instance into one or more optimized concrete MongoDB queries.
     *
     * Firstly, the query is optimized, which may generate a top-level UNION.
     * Then, a query string is generated from the optimized MongoQueryNode, to which the initial query string
     * from the logical source is appended.
     * A UNION is translated into several concrete queries. For any other type of query only one query string is returned.
     *
     * @TODO the project part is not managed: must transform JSONPath references nito actually projectable
     * fields in a MongoDB query
     *
     * @param from from part of the atomic abstract query (query from the logical source)
     * @param project project part of the atomic abstract query (xR2RML references to project, NOT MANAGED FOR NOW).
     * @param absQuery the abstract MongoDB query to translate into concrete MongoDB queries.
     * @return list of MongoDBQuery instances. If there are several instances, their results must be UNIONed.
     */
    def mongoAbstractQuerytoConcrete(
        from: MongoDBQuery,
        project: List[MorphBaseQueryProjection],
        absQuery: MongoQueryNode): List[MongoDBQuery] = {

        // If there are more than 1 query, encapsulate them under a top-level AND and optimize the resulting query
        var Q = absQuery.optimize
        if (logger.isTraceEnabled())
            logger.trace("Condtion set was translated into: " + Q)

        // If the query is an AND, merge its FIELD nodes that have the same path
        // Example 1. AND('a':{$gt:10}, 'a':{$lt:20}) => 'a':{$gt:10, $lt:20}
        // Example 2. AND('a.b':{$elemMatch:{Q1}}, 'a.b':{$elemMatch:{Q2}}) => 'a.b': {$elemMatch:{$and:[{Q1},{Q2}]}}.
        if (Q.isAnd)
            Q = new MongoQueryNodeAnd(MongoQueryNode.fusionQueries(Q.asInstanceOf[MongoQueryNodeAnd].members))

        val Qstr: List[MongoDBQuery] =
            if (Q.isUnion)
                // For each member of the UNION, convert it to a query string, add the query from the logical source,
                // and create a MongoDBQuery instance
                Q.asInstanceOf[MongoQueryNodeUnion].members.map(
                    q => new MongoDBQuery(
                        from.collection,
                        q.toTopLevelQuery(from.query),
                        q.toTopLevelProjection))
            else
                // If no UNION, convert to a query string, add the query from the logical source, and create a MongoDBQuery instance
                List(new MongoDBQuery(
                    from.collection,
                    Q.toTopLevelQuery(from.query),
                    Q.toTopLevelProjection))

        if (logger.isTraceEnabled())
            logger.trace("Final set of concrete queries: [" + Qstr + "]")
        Qstr
    }

    /**
     * Generate the data source for the parent triples map of the triples map passed
     *
     * @param tm a triples map that has been assessed as a candidate triples map for the translation of tp into a query
     * @return a MongoDBQuery representing the query string of the parent triples map
     */
    def genFromParent(tm: R2RMLTriplesMap): MongoDBQuery = {
        val pom = tm.getPropertyMappings.head

        if (pom.hasRefObjectMap) {
            val rom = pom.getRefObjectMap(0)
            val parentTMLogSrc = md.getParentTriplesMap(rom).getLogicalSource
            val query = MongoDBQuery.parseQueryString(parentTMLogSrc.getValue, parentTMLogSrc.docIterator, true)
            query
        } else
            throw new MorphException("Triples map " + tm + " has no parent triples map")
    }
}
