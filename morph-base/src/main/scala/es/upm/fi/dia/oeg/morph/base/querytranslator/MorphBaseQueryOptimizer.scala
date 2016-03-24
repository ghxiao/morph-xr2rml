package es.upm.fi.dia.oeg.morph.base.querytranslator

class MorphBaseQueryOptimizer {
    var selfJoinElimination = true
    var selfUnionElimination = true
    var propagateConditionFromJoin = true

    var transJoinSubQueryElimination = true
    var transSTGSubQueryElimination = true
    var unionQueryReduction = true
    var subQueryElimination = true
    var subQueryAsView = false
}