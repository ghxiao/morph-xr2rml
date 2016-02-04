package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryOptimizer

class MorphRDBQueryOptimizer extends MorphBaseQueryOptimizer {

    var selfJoinElimination = true;
    var transJoinSubQueryElimination = true;
    var transSTGSubQueryElimination = true;
    var unionQueryReduction = true;
    var subQueryElimination = true;
    var subQueryAsView = false;
}