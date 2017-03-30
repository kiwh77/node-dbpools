/*!
 * 
 * Copyright(c) 2016 huangbinglong
 * MIT Licensed
 */

'use strict';

function Condition() {

    this.conditions = [];
    this.orderBys = [];

    /**
     * name
     * operator 看Condition.OPERATOR_*
     * value
     * @param opts
     */
    this.addCondition = function(opts) {
        if (!opts || !opts.name || !opts.operator) {
            throw "缺乏必要参数.";
        }
        this.conditions.push(opts);
    };

    /**
     * name
     * direction : desc或者asc
     * @param opts
     */
    this.addOrderBy = function(opts) {
        if (!opts || !opts.name) {
            throw "缺乏必要参数.";
        }
        opts.direction = opts.direction || "asc";
        this.orderBys.push(opts);
    };

    this.setPage = function(pageNumber,pageSize) {
        this.pageNumber = pageNumber,
        this.pageSize = pageSize
    };

    this.hasOrderBy = function() {
        return this.orderBys.length > 0;
    };

    this.hasTop = function() {
        return this.pageSize > 0;
    };

    this.getConditions = function() {
        var str = "";
        var cond = null;
        for (var i = 0;i < this.conditions.length;i++) {
            cond = this.conditions[i];
            str += " AND "+cond.name;
            if (cond.operator == Condition.OPERATOR_EQ) {
                if (typeof cond.value == 'string') {
                    str += "='"+cond.value+"'";
                } else {
                    str += "="+cond.value+""
                }

            } else if (cond.operator == Condition.OPERATOR_LIKE) {
                str += " like '%"+cond.value+"%'"
            } else if (cond.operator == Condition.OPERATOR_IN) {
                str += " in (";
                for (var j = 0;j < cond.value.length;j++) {
                    str += "'"+cond.value[i]+"'";
                    if (j != cond.value.length-1) {
                        str += ","
                    }
                }
                str += ")";
            } else {
                throw "无法识别操作符："+cond.operator;
            }
        }
        var orderBy = null;
        if (this.orderBys.length > 0) {
            str += " ORDER BY ";
        }
        for (var i = 0;i < this.orderBys.length;i++) {
            orderBy = this.orderBys[i];
            str += orderBy.name+" "+orderBy.direction;
            if (i != this.orderBys.length-1) {
                str += ","
            }
        }
        return str;
    };
}


// 字段条件
Condition.OPERATOR_EQ = "eq";
Condition.OPERATOR_LIKE = "like";
Condition.OPERATOR_IN = "in";

// 排序方向
Condition.DIRECTION_DESC = "desc";
Condition.DIRECTION_ASC = "asc";



module.exports = Condition;
