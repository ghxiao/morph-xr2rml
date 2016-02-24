package fr.unice.i3s.morph.xr2rml.mongo.abstractquery

import org.apache.log4j.Logger

import com.hp.hpl.jena.graph.Triple

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultRdfTerms
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQuery
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryCondition
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryProjection
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoDataTranslator
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoResultSet

/**
 * Representation of the abstract atomic query as defined in https://hal.archives-ouvertes.fr/hal-01245883
 *
 * @param triple the triple pattern for which we create this atomic query
 * @param boundTriplesMap the triples map, bound to the triple pattern, from which this query is derived
 * @param from consists of the triples map logical source
 * @param project set of xR2RML references that shall be projected in the target query, i.e. the references
 * needed to generate the RDF terms of the result triples
 * @param where set of conditions applied to xR2RML references, entailed by matching the triples map
 * with the triple pattern.
 */
class MorphAbstractAtomicQuery(

    val triple: Option[Triple],
    boundTriplesMap: Option[R2RMLTriplesMap],
    val from: xR2RMLLogicalSource,
    val project: List[MorphBaseQueryProjection],
    val where: List[MorphBaseQueryCondition])

        extends MorphAbstractQuery(boundTriplesMap) {

    val logger = Logger.getLogger(this.getClass().getName());

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
     * Return the list of SPARQL variables projected in this abstract query
     */
    override def getVariables: List[String] = {
        project.filter(_.as.isDefined).map(_.as.get).sortWith(_ < _).distinct
    }

    /**
     * Execute the query and produce the RDF terms for each of the result documents
     * by applying the triples map bound to this query.
     * If targetQuery contains several queries their result is UNIONed.
     *
     * @param dataSourceReader the data source reader to query the database
     * @param dataTrans the data translator to create RDF terms
     * @return a list of MorphBaseResultRdfTerms instances, one for each result document
     * May return an empty result but NOT null.
     */
    override def generateRdfTerms(
        dataSourceReader: MorphBaseDataSourceReader,
        dataTrans: MorphBaseDataTranslator): List[MorphBaseResultRdfTerms] = {

        if (!triple.isDefined || !boundTriplesMap.isDefined) {
            val errMsg = "Atomic abstract query with either no associtaed triple pattern or no triples map bound to the triple pattern" +
                " => the query is a child or parent query of a RefObjectMap. Cannot call the generatedRdfTerms method."
            logger.error(errMsg)
            logger.error("Query: " + this.toString)
            throw new MorphException(errMsg)
        }

        val tp = triple.get
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
        val tm = this.boundTriplesMap.get
        val sm = tm.subjectMap;
        val pom = tm.predicateObjectMaps.head
        val iter: Option[String] = tm.logicalSource.docIterator
        logger.info("Generating RDF terms from atomic query below into under triples map " + tm.toString + ": \n" + this.toStringConcrete);

        // Execute the queries of tagetQuery and make a UNION (flatMap) of all the results
        val resSets = this.targetQuery.map(query => dataSourceReader.executeQueryAndIterator(query, iter))
        val resultSet = resSets.flatMap(res => res.asInstanceOf[MorphMongoResultSet].resultSet)

        // Main loop: iterate and process each result document of the result set
        var i = 0;
        val terms = for (document <- resultSet) yield {
            try {
                i = i + 1;
                if (logger.isDebugEnabled()) logger.debug("Generating RDF terms for document " + i + "/" + resultSet.size + ": " + document)

                //---- Create the subject resource
                val subjects = dataTranslator.translateData(sm, document)
                if (subjects == null) { throw new Exception("null value in the subject triple") }
                if (logger.isDebugEnabled()) logger.debug("Document " + i + " subjects: " + subjects)

                //---- Create the list of resources representing subject target graphs
                val subjectGraphs = sm.graphMaps.flatMap(sgmElement => {
                    val subjectGraphValue = dataTranslator.translateData(sgmElement, document)
                    val graphMapTermType = sgmElement.inferTermType;
                    graphMapTermType match {
                        case Constants.R2RML_IRI_URI => { subjectGraphValue }
                        case _ => {
                            val errorMessage = "GraphMap's TermType is not valid: " + graphMapTermType;
                            logger.warn(errorMessage);
                            throw new MorphException(errorMessage);
                        }
                    }
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
                    val poGraphValue = dataTranslator.translateData(pogmElement, document)
                    poGraphValue
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
        terms.flatten // get rid of the None's (in case there was an exception)
    }
}