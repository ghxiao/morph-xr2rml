package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions.asJavaCollection
import scala.collection.JavaConversions.setAsJavaSet
import org.apache.log4j.Logger
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.algebra.optimize.Optimize
import com.hp.hpl.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.querytranslator.engine.MorphQueryRewritterFactory
import es.upm.fi.dia.oeg.morph.base.sql.ISqlQuery
import es.upm.fi.dia.oeg.morph.base.sql.SQLUnion
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.base.GenericQuery
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.algebra.Algebra
import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.sparql.algebra.Op

abstract class MorphBaseQueryTranslator extends IQueryTranslator {

    val logger = Logger.getLogger(this.getClass());

    /** Map of nodes of a SPARQL query and the candidate triples maps for each node **/
    var mapInferredTMs: Map[Node, Set[R2RMLTriplesMap]] = Map.empty;

    var mapTripleAlias: Map[Triple, String] = Map.empty;

    /** Set of not-null conditions for variable in a SPARQL query */
    var mapVarNotNull: Map[Node, Boolean] = Map.empty;

    Optimize.setFactory(new MorphQueryRewritterFactory());

    /**
     * High level method to start the translation process
     */
    def translate(sparqlQuery: Query): GenericQuery = {

        val start = System.currentTimeMillis();

        val result = this.translate(Algebra.compile(sparqlQuery));
        logger.info("Query translation time = " + (System.currentTimeMillis() - start) + "ms.");
        result
    }

    protected def translate(op: Op): GenericQuery

    /**
     * Translation of a triple pattern into a union of SQL queries,
     * with one query in the union for each candidate triples map.
     */
    protected def transTP(tp: Triple): ISqlQuery = {
        val tpSubject = tp.getSubject();
        val tpPredicate = tp.getPredicate();
        val tpObject = tp.getObject();

        val skipRDFTypeStatement = false;

        val result = {
            if (tpPredicate.isURI() && RDF.`type`.getURI().equals(tpPredicate.getURI())
                && tpObject.isURI() && skipRDFTypeStatement) {
                null;
            } else {
                // Get the candidate triple maps for the subject of the triple pattern 
                val cmsOption = this.mapInferredTMs.get(tpSubject);
                val cms = {
                    if (cmsOption.isDefined) {
                        cmsOption.get;
                    } else {
                        logger.warn("Undefined triplesMap for triple : " + tp + ". All triple patterns will be used");
                        val cmsAux = this.mappingDocument.classMappings;
                        if (cmsAux == null || cmsAux.size() == 0)
                            logger.warn("Mapping document doesn't contain any triple pattern mapping");
                        cmsAux.toSet
                    }
                }

                // Build a union of per-triples-map queries
                val unionOfQueries = cms.flatMap(cm => {
                    val resultAux = this.transTP(tp, cm);
                    if (resultAux != null) {
                        Some(resultAux);
                    } else
                        None
                })

                if (unionOfQueries.size() == 0)
                    null;
                else if (unionOfQueries.size() == 1)
                    unionOfQueries.head;
                else if (unionOfQueries.size() > 1)
                    SQLUnion(unionOfQueries);
                else
                    null
            }
        }
        result;
    }

    /**
     * Translation of a triple pattern into a SQL query based on a candidate triples map.
     */
    protected def transTP(tp: Triple, cm: R2RMLTriplesMap): ISqlQuery
}