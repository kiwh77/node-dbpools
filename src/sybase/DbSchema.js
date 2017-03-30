/*!
 * 
 * Copyright(c) 2016 huangbinglong
 * MIT Licensed
 */

'use strict';

var Conditions = require("./DbConditions");

var pool = require('./DbPool');

var Schema = function (tableName,schema) {
    // SQL模板
    var SQL_INSERT_PATTERN = "INSERT INTO "+tableName+"($COLUMNS) VALUES($VALUES)";
    var SQL_UPDATE_PATTERN = "update "+tableName+" set $VALUES WHERE 1=1 $CONDITIONS";
    var SQL_QUERY_PATTERN = "SELECT $TOP $COLUMNS FROM "+tableName+" WHERE 1=1 $CONDITIONS";
    var SQL_DELETE_PATTERN = "DELETE FROM "+tableName+" WHERE $CONDITIONS";

    var idColumn = schema['$id'] || 'id';
    delete schema['$id'];

    var target = this;
    target.tableName =tableName;
    target.schema = schema;
    target.ignoreWords = {update:[],save:[]}; //忽略字段，更新或新增时忽略, update,save

    target.getColumns = function(ignore) {
        var columns = "";
        for (var col in target.schema) {
            if (ignore){
                if ((target.ignoreWords.save.length && target.ignoreWords.save.indexOf(col)>-1) ||
                    (target.ignoreWords.update.length && target.ignoreWords.update.indexOf(col)>-1)){
                    continue;
                }
            }
            columns += ""+col+",";
        }
        if (columns) {
            columns = columns.substring(0,columns.length -1);
        }
        return columns;
    };

    target.methods = {}; // 内置方法都用$开头


    // 获取字段的值
    target.methods.getColumnValues = function(params, ignore) {
        var values = "";
        var val = '';
        for (var col in target.schema) {
            if (ignore){
                if ((target.ignoreWords.save.length && target.ignoreWords.save.indexOf(col)>-1) ||
                    (target.ignoreWords.update.length && target.ignoreWords.update.indexOf(col)>-1)){
                    continue;
                }
            }
            val = params[col];
            if (typeof val == "undefined") {
                values += "null,"
            } else if (target.schema[col] == Schema.TYPE_STRING) {
                values += "'"+val+"',";
            } else if (target.schema[col] == Schema.TYPE_INT) {
                values += val+",";
            } else if (target.schema[col] == Schema.TYPE_DATE) {
                values += "'"+val+"',"; // 这里还是前端自己格式化保存吧
            }else if (target.schema[col] == schema.TYPE_NUMERIC){
                values += val+',';
            }else{
                values += val+',';
            }
        }
        if (values) {
            values = values.substring(0,values.length -1);
        }

        return values;
    };

    target.methods.getModifyColums = function(params, ignore) {
        var ms = [];
        for (var col in target.schema) {
            if (ignore){
                if ((target.ignoreWords.save.length && target.ignoreWords.save.indexOf(col)>-1) ||
                    (target.ignoreWords.update.length && target.ignoreWords.update.indexOf(col)>-1)){
                    continue;
                }
            }
            var val = params[col] || this[col];
            if (typeof val != "undefined") {
                ms.push({
                    name:col,
                    val:val
                });
            }
        }
        return ms;
    };

    // 添加
    target.methods.$save = function(params,callback) {
        var model = this;
        params = params || {};
        if (this.originData && this.originData[idColumn]) {
            return this.$update(params,callback);
        }
        var sql = SQL_INSERT_PATTERN;
        sql = sql.replace("$COLUMNS",target.getColumns(true));
        sql = sql.replace("$VALUES",this.getColumnValues(params, true));
        pool.execute(sql,function(err,data) {
            if (err) {
                callback(err,data);
            } else {
                var ps = {};
                ps[idColumn] = model[idColumn] || params[idColumn];
                //当主键为空时，通过其它参数查询信息
                if (!ps[idColumn]){
                    ps = params;
                }
                model.$findOne(ps,callback);
            }
        });
    };

    // 更新
    target.methods.$update = function(params,callback) {
        var model = this;
        params = params || {};
        var sql = SQL_UPDATE_PATTERN;
        var id = this.originData[idColumn];
        var replaceValue = "AND "+idColumn +'=';
        if (this[idColumn] == this.TYPE_NUMERIC) {
            if (typeof id != 'number'){
                id = parseInt(id);
            }
            replaceValue += id
        }else{
            replaceValue += "'" + id + "'";
        }

        sql = sql.replace("$CONDITIONS",replaceValue);
        var ms  = this.getModifyColums(params, true);
        var setValues = "";
        if (ms.length == 0) { return; }
        //如果值是字符类型，则需要加上' ，数字类型不能加
        for (var i = 0;i < ms.length;i++) {
            var item = ms[i];
            if (typeof item.val == 'string'){
                item.val = "'" + item.val + "'";
            }
            setValues += ms[i].name+"="+item.val+",";
        }
        if (setValues.length){
            setValues = setValues.substr(0,setValues.length-1);
        }
        sql = sql.replace("$VALUES",setValues);
        pool.execute(sql,function(err,data) {
            if (err) {
                callback(err,data);
            } else {
                var ps = {};
                ps[idColumn] = model.originData[idColumn];
                model.$findOne(ps,callback);
            }
        });
    };

    // 删除
    target.methods.$delete = function(callback) {
        var model = this;
        var sql = SQL_DELETE_PATTERN;
        sql = sql.replace("$CONDITIONS"," "+idColumn+"='"+this.originData[idColumn]+"'");
        pool.execute(sql,function(err,data) {
            if (err) {
                callback(err,data);
            } else {
                callback(err,model.originData);
            }
        });
    };

    // 查询单个
    target.methods.$findOne = function(params,callback) {
        params = params || {};
        var model =this;
        var conditions = new Conditions();
        for (var k in params) {
            var p = {};
            p.name = k;
            p.operator = Conditions.OPERATOR_EQ;
            p.value=params[k];
            conditions.addCondition(p);
        }

        this.$search(conditions,function(err,data) {
            if (!err && data) {
                model.originData = data[0];
            }
            callback(err,data);
        });
    };

    // 查询列表
    target.methods.$search = function(conditions,callback) {
        var sql = SQL_QUERY_PATTERN;
        var condition;
        if (conditions.constructor !== Conditions){
            condition = new Conditions();
            for (var key in conditions){
                var p = {};
                p.name = key;
                p.operator = Conditions.OPERATOR_EQ;
                p.value = conditions[key];
                condition.addCondition(p);
            }
        }else{
            condition = conditions;
        }
        sql = sql.replace("$COLUMNS",target.getColumns());
        sql = sql.replace("$CONDITIONS",condition.getConditions());
        if (condition.hasTop()) {
            sql = sql.replace("$TOP","top "+condition.pageSize);
        } else {
            sql = sql.replace("$TOP","");
        }
        pool.execute(sql,callback);
    };

    target.model = function (params) {
        this.originData = {};// 原始数据
        for (var k in target.methods) {
            this[k] = target.methods[k];
        }
        params = params || {};
        for (var k in params) {
            this[k] = params[k];
        }
    };
};

// 支持的数据类型
Schema.TYPE_STRING = "string";
Schema.TYPE_INT = "int";
Schema.TYPE_DATE = "date";
Schema.TYPE_NUMERIC ="double";

module.exports = Schema;