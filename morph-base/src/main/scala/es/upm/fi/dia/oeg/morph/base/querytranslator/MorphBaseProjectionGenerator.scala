package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.LinkedHashSet

import org.apache.log4j.Logger

import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF

import Zql.ZSelectItem
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

abstract class MorphBaseProjectionGenerator {

    var mapHashCodeMapping: Map[Integer, Object] = Map.empty

    def getMappedMapping(hashCode: Integer) = {
        this.mapHashCodeMapping.get(hashCode);
    }

    def putMappedMapping(key: Integer, value: Object) {
        this.mapHashCodeMapping += (key -> value);
    }
}