package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions.asJavaCollection
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.mutableSetAsJavaSet
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.JavaConversions.setAsJavaSet
import scala.collection.mutable.LinkedHashSet
import org.apache.log4j.Logger
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.algebra.Algebra
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct
import com.hp.hpl.jena.sparql.algebra.op.OpExtend
import com.hp.hpl.jena.sparql.algebra.op.OpFilter
import com.hp.hpl.jena.sparql.algebra.op.OpGroup
import com.hp.hpl.jena.sparql.algebra.op.OpJoin
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin
import com.hp.hpl.jena.sparql.algebra.op.OpOrder
import com.hp.hpl.jena.sparql.algebra.op.OpProject
import com.hp.hpl.jena.sparql.algebra.op.OpSlice
import com.hp.hpl.jena.sparql.algebra.op.OpUnion
import com.hp.hpl.jena.sparql.algebra.optimize.Optimize
import com.hp.hpl.jena.sparql.core.BasicPattern
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.sparql.expr.E_Bound
import com.hp.hpl.jena.sparql.expr.E_Function
import com.hp.hpl.jena.sparql.expr.E_LogicalAnd
import com.hp.hpl.jena.sparql.expr.E_LogicalNot
import com.hp.hpl.jena.sparql.expr.E_LogicalOr
import com.hp.hpl.jena.sparql.expr.E_NotEquals
import com.hp.hpl.jena.sparql.expr.E_OneOf
import com.hp.hpl.jena.sparql.expr.E_Regex
import com.hp.hpl.jena.sparql.expr.Expr
import com.hp.hpl.jena.sparql.expr.ExprFunction
import com.hp.hpl.jena.sparql.expr.ExprFunction1
import com.hp.hpl.jena.sparql.expr.ExprFunction2
import com.hp.hpl.jena.sparql.expr.ExprList
import com.hp.hpl.jena.sparql.expr.NodeValue
import com.hp.hpl.jena.sparql.expr.aggregate.AggAvg
import com.hp.hpl.jena.sparql.expr.aggregate.AggCount
import com.hp.hpl.jena.sparql.expr.aggregate.AggMax
import com.hp.hpl.jena.sparql.expr.aggregate.AggMin
import com.hp.hpl.jena.sparql.expr.aggregate.AggSum
import com.hp.hpl.jena.vocabulary.RDF
import com.hp.hpl.jena.vocabulary.XSD
import Zql.ZConstant
import Zql.ZExp
import Zql.ZExpression
import Zql.ZGroupBy
import Zql.ZOrderBy
import Zql.ZSelectItem
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.querytranslator.engine.MorphMappingInferrer
import es.upm.fi.dia.oeg.morph.base.querytranslator.engine.MorphQueryRewriter
import es.upm.fi.dia.oeg.morph.base.querytranslator.engine.MorphQueryRewritterFactory
import es.upm.fi.dia.oeg.morph.base.querytranslator.engine.MorphQueryTranslatorUtility
import es.upm.fi.dia.oeg.morph.base.querytranslator.engine.MorphSQLSelectItemGenerator
import es.upm.fi.dia.oeg.morph.base.sql.ISqlQuery
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.sql.SQLFromItem
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLQuery
import es.upm.fi.dia.oeg.morph.base.sql.SQLUnion
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import java.sql.Connection

abstract class MorphBaseQueryTranslator extends IQueryTranslator {

    val logger = Logger.getLogger(this.getClass());

    /** Map of nodes of a SPARQL query and the candidate triples maps for each node **/
    var mapInferredTMs: Map[Node, Set[R2RMLTriplesMap]] = Map.empty;

    var mapTripleAlias: Map[Triple, String] = Map.empty;

    /** Set of not-null conditions for variable in a SPARQL query */
    var mapVarNotNull: Map[Node, Boolean] = Map.empty;

    Optimize.setFactory(new MorphQueryRewritterFactory());

    /**
     * Translation of a triple pattern into a union of SQL queries,
     * with one query in the union for each candidate triples map.
     */
    def transTP(tp: Triple): ISqlQuery = {
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
                val unionOfSQLQueries = cms.flatMap(cm => {
                    val resultAux = this.transTP(tp, cm);
                    if (resultAux != null) {
                        Some(resultAux);
                    } else
                        None
                })

                if (unionOfSQLQueries.size() == 0)
                    null;
                else if (unionOfSQLQueries.size() == 1)
                    unionOfSQLQueries.head;
                else if (unionOfSQLQueries.size() > 1)
                    SQLUnion(unionOfSQLQueries);
                else
                    null
            }
        }
        result;
    }

    /**
     * Translation of a triple pattern into a SQL query based on a candidate triples map.
     */
    def transTP(tp: Triple, cm: R2RMLTriplesMap): ISqlQuery

}