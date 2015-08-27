package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions.asJavaCollection

import org.apache.log4j.Logger

import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF

import Zql.ZConstant
import Zql.ZSelectItem
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

abstract class MorphBaseBetaGenerator(md: R2RMLMappingDocument, unfolder: MorphBaseUnfolder) {
    val logger = Logger.getLogger(this.getClass());

    val dbType = if (md.dbMetaData.isDefined) { md.dbMetaData.get.dbType; }
    else { Constants.DATABASE_DEFAULT; }

    val alphaGenerator: MorphBaseAlphaGenerator = null;
    var owner: MorphBaseQueryTranslator = null;

    def calculateBeta(tp: Triple, pos: Constants.MorphPOS.Value, cm: R2RMLTriplesMap, predicateURI: String, alphaResult: MorphAlphaResult): List[ZSelectItem] = {
        val result: List[ZSelectItem] = {
            if (pos == Constants.MorphPOS.sub) {
                this.calculateBetaSubject(tp, cm, alphaResult).toList;
            } else if (pos == Constants.MorphPOS.pre) {
                List(this.calculateBetaPredicate(predicateURI));
            } else if (pos == Constants.MorphPOS.obj) {
                val predicateIsRDFSType = RDF.`type`.getURI().equals(predicateURI);
                if (predicateIsRDFSType) {
                    val className = new ZConstant(cm.getConceptName(), ZConstant.STRING);
                    val selectItem = MorphSQLSelectItem.apply(className);
                    //selectItem.setExpression(className);
                    List(selectItem);
                } else {
                    this.calculateBetaObject(tp, cm, predicateURI, alphaResult).toList;
                }
            } else {
                throw new Exception("invalid Pos value in beta!");
            }
        }

        logger.debug("beta = " + result);
        return result;
    }

    def calculateBetaObject(triple: Triple, cm: R2RMLTriplesMap, predicateURI: String, alphaResult: MorphAlphaResult): List[ZSelectItem] = {
        val betaObjects: List[ZSelectItem] = {
            val pms = cm.getPropertyMappings(predicateURI);
            if (pms == null || pms.isEmpty()) {
                val errorMessage = "Undefined mappings for : " + predicateURI + " for class " + cm.getConceptName();
                logger.debug(errorMessage);
                Nil;
            } else if (pms.size() > 1) {
                val errorMessage = "Multiple property mappings defined, result may be wrong!";
                logger.debug(errorMessage);
                throw new Exception(errorMessage);
            } else { //if(pms.size() == 1)
                val pm = pms.iterator.next();
                this.calculateBetaObject(triple, cm, predicateURI, alphaResult, pm).toList;
            }
        }
        return betaObjects;
    }

    def calculateBetaObject(triple: Triple, cm: R2RMLTriplesMap, predicateURI: String, alphaResult: MorphAlphaResult, pm: R2RMLPredicateObjectMap): List[ZSelectItem];

    def calculateBetaPredicate(predicateURI: String): ZSelectItem = {
        val predicateURIConstant = new ZConstant(predicateURI, ZConstant.STRING);
        val selectItem = MorphSQLSelectItem.apply(predicateURIConstant);
        selectItem;
    }

    def calculateBetaSubject(tp: Triple, cm: R2RMLTriplesMap, alphaResult: MorphAlphaResult): List[ZSelectItem];

}