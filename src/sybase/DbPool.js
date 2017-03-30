/*!
 *
 * 数据库连接，线程池
 * Copyright(c) 2016 huangbinglong
 */

'use strict';

var DbDriver = require('sybase');

var DdPool = function (opts) {
    var pool = this;
    pool._connections = [];
    pool._connectionNums = 0;
    pool._busyConnections = {};
    pool._waitCallbacks = [];
    pool._busyConnectionNums = 0;

    pool._release = function(client){
        if (client.isUseing){
            client.callback = true;
        }else{
            client.disconnect();
            var index = pool._connections.indexOf(client);
            if (index > -1){
                pool._connections.splice(1, index);
                pool._connectionNums--;
            }
        }
    };

    

    pool._createConnection = function() {
        pool._connectionNums++;
        var client = new DbDriver(
            process.env.SYBASE_HOST,
            process.env.SYBASE_PORT,
            process.env.SYBASE_DB_NAME,
            process.env.SYBASE_USER_NAME,
            process.env.SYBASE_PWD,
            true,
            __dirname+'/../node_modules/sybase/JavaSybaseLink/dist/JavaSybaseLink.jar');
        client.connect(function (err) {
            if (err) return console.log(err);
            pool._connections.push(client);
            console.log("成功创建连接，总连接数："+pool._connections.length);
            pool._executeWaitCallback();
        });
        client.isUseing = false;
        client.callback = false;
        return client;
    };

    for (var i = 0;i < opts.min;i++) {
        pool._createConnection();
    }

    pool._getConnection = function() {
        var conn = null;
        var index = 0;
        for (var i = 0;i < pool._connections.length;i++) {
            if (!pool._busyConnections[i]) {
                index = i;
                conn = pool._connections[i];
                break;
            }
        }
        if (conn) {
            if (conn.isConnected()) {
                pool._busyConnections[index] = conn;
                pool._busyConnectionNums++;
                return [index,conn];
            } else {
                console.log("连接已关闭.."+index);
                pool._connectionNums--;
                pool._connections.splice(index,1);
                return pool._getConnection();
            }

        } else if (opts.max > pool._connectionNums){
            pool._createConnection();
        }
    };

    pool._executeWaitCallback = function() {
      if (pool._waitCallbacks.length > 0) {
          console.log("执行等待中的连接请求..总数："+pool._waitCallbacks.length);
      }
      for (var i =0;i < pool._waitCallbacks.length && i < pool._getFreeConnectionNums();i++) {
          var waitC = pool._waitCallbacks.shift();
          pool.execute(waitC.sql,waitC.callback);
      }
    };

    pool._getFreeConnectionNums = function () {
        return pool._connections.length - pool._busyConnectionNums;
    };
    
    pool.execute = function(sql,callback) {
        const connInfo = pool._getConnection();
        if (connInfo) {
            console.log("通过连接["+connInfo[0]+"]执行..");
            connInfo[1].isUseing = true;
            connInfo[1].query(sql, function (err, data) {
                if (err) console.log(err);
                if (err && err.message.indexOf("JZ0CU") > -1) {
                    callback(null,data);
                } else {
                    callback(err, data);
                }
                // 释放连接
                connInfo[1].isUseing = false;
                if (connInfo[1].callback){
                    pool._release(connInfo[1]);
                }
                pool._busyConnectionNums--;
                delete pool._busyConnections[connInfo[0]];

                pool._executeWaitCallback();
            });
        } else {
            pool._waitCallbacks.push({sql:sql,callback:callback});
        }
    };
};

module.exports = new DdPool({
    max: 10, // 最大连接数
    min: 2 // 最小连接数
});