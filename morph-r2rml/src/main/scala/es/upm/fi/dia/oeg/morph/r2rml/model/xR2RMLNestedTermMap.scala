package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._

import org.apache.log4j.Logger

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElement
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor

class xR2RMLNestedTermMap(
        termType: Option[String],
        parseType: Option[String],
        recursive_parse: xR2RMLNestedTermMap,
        val datatype: Option[String],
        val languageTag: Option[String]) {

    val logger = Logger.getLogger(this.getClass().getName());

    var parset = this.getParseType.get
    var ter = this.getTermType

    //  checking the property termType
    if (ter != null) {
        if (!ter.equals(Constants.R2RML_BLANKNODE_URI) && !ter.equals(Constants.R2RML_LITERAL_URI) &&
            !ter.equals(Constants.R2RML_IRI_URI) && !ter.equals(xR2RML_Constants.xR2RML_RDFALT_URI) &&
            !ter.equals(xR2RML_Constants.xR2RML_RDFBAG_URI) && !ter.equals(xR2RML_Constants.xR2RML_RDFSEQ_URI) &&
            !ter.equals(xR2RML_Constants.xR2RML_RDFLIST_URI)) { throw new Exception("Illegal termtype value in parse \n" + ter + "  is not a correct value"); }
    }

    def getlanguageTag() = {
        if (this.languageTag != null && this.languageTag.isDefined) {
            this.languageTag
        } else {
            None
        }
    }
    def hasRecursiveParse(): Boolean = {
        if (this.recursive_parse == null) {
            return false
        } else {
            return true
        }
    }

    def getdatatype() = {
        if (this.datatype != null && this.datatype.isDefined) {
            this.datatype
        } else {
            None
        }
    }

    def getParseType() = {
        if (this.parseType != null) {
            this.parseType
        } else {
            Some(Constants.R2RML_LITERAL_URI)
        }
    }

    def getRecursiveParse() = {
        recursive_parse
    }

    def getTermType(): String = {
        if (this.termType != null) {
            this.termType.get
        } else {
            Constants.R2RML_LITERAL_URI
        }

    }

}

