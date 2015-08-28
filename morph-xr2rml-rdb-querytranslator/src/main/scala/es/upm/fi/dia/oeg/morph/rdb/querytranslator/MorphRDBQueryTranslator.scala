package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import java.util.regex.Matcher

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.setAsJavaSet

import org.apache.log4j.Logger

import com.hp.hpl.jena.graph.Node

import Zql.ZConstant
import Zql.ZExp
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseAlphaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseBetaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseCondSQLGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBasePRSQLGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.NameGenerator

class MorphRDBQueryTranslator(
    nameGenerator: NameGenerator,
    alphaGenerator: MorphBaseAlphaGenerator,
    betaGenerator: MorphBaseBetaGenerator,
    condSQLGenerator: MorphBaseCondSQLGenerator,
    val prSQLGenerator: MorphBasePRSQLGenerator)

        extends MorphBaseQueryTranslator(
            nameGenerator: NameGenerator,
            alphaGenerator: MorphBaseAlphaGenerator,
            betaGenerator: MorphBaseBetaGenerator,
            condSQLGenerator: MorphBaseCondSQLGenerator,
            prSQLGenerator: MorphBasePRSQLGenerator) {

    override val logger = Logger.getLogger(this.getClass());

    this.alphaGenerator.owner = this;
    this.betaGenerator.owner = this;

    private var mapTemplateMatcher: Map[String, Matcher] = Map.empty;
    private var mapTemplateAttributes: Map[String, List[String]] = Map.empty;

    /**
     * For a node representing a URI, get the first (why?) triples map candidate of that node,
     * and return the list of values from the URI that match the columns in the template string
     * of the subject map of that triples map.
     */
    override def transIRI(node: Node): List[ZExp] = {
        val cms = mapInferredTMs(node);
        val cm = cms.iterator().next();
        val mapColumnsValues = cm.subjectMap.getTemplateValues(node.getURI());
        val result: List[ZExp] = {
            if (mapColumnsValues == null || mapColumnsValues.size() == 0) {
                //do nothing
                Nil
            } else {
                val resultAux = mapColumnsValues.keySet.map(column => {
                    val value = mapColumnsValues(column);
                    val constant = new ZConstant(value, ZConstant.UNKNOWN);
                    constant;
                })
                resultAux.toList;
            }
        }
        result;
    }
}
