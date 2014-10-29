var vm = require('vm');
var async = require('async');
var _ = require('lodash');

exports.find = function (records, criteria, callback){
    var boundFunc = _.bind(_tryMatch, {criteria: criteria});
    async.filter(records, boundFunc, function(results){
        return callback(results);
    });
}

exports.match = function (record, criteria, callback){
    var boundFunc = _.bind(_tryMatch, {criteria: criteria});
    boundFunc(record, function(result){
        return callback(result);
    });
}

function _tryMatch(record, recordCallback){
    _doEvery(record, this.criteria, recordCallback);
}

function _doEvery(record, criteria, cb){
    var boundFunc = _.bind(_checkTrue, {record: record, criteria: criteria});
    async.every(_.keys(criteria), boundFunc, function(result){
        return cb(result);
    });
}

function _callNor(record, criteria, cb){
    // reverse result of OR
    _callOr(record, criteria, function(result){
        return cb(!result);      
    });     
}

function _callOr(record, criteria, cb){
    var boundFunc = _.bind(_checkTrue, {record: record, criteria: criteria});
    async.detect(_.keys(criteria), boundFunc, function(innerResult){
        return cb(innerResult);
    });
}

function _checkTrue(key, keyCallback){
    var currCriteria = this.criteria[key];
    if (currCriteria instanceof Array){
        if (key === '$and'){
            return _doEvery(this.record, currCriteria, keyCallback);
        }
        else if (key === '$or'){
            return _callOr(this.record, currCriteria, keyCallback);
        }
        else if (key === '$nor'){
            return _callNor(this.record, currCriteria, keyCallback);
        }
    }
    else if (key === '$where'){
        var contextCode = '';
        var recordKeys = _.keys(this.record);
        for (var k = 0; k < recordKeys.length; k++){
            contextCode += 'var ' + recordKeys[k] + '=' + JSON.stringify(this.record[recordKeys[k]]) + ';';
        }

        var retValue = false;
        try {
            retValue = vm.runInThisContext(contextCode + this.criteria[key]);
        }
        catch(e){
            //console.log('Error in $where clause: ' + e);
        }
        keyCallback(retValue);
    }
    else if (typeof currCriteria === 'object'){
        var currCriteriaLength = _.keys(currCriteria).length;
        var noMatch = false;
        if (currCriteriaLength == 1){
            var retValue = false;
            switch (_.keys(currCriteria)[0]){
                case '$exists':
                    var isDef = (typeof _objAccessor(this.record, key) != 'undefined');
                    retValue = currCriteria['$exists'] ? isDef : !isDef;
                    break;
                case '$lt':
                    retValue = (_objAccessor(this.record, key) < currCriteria['$lt']);
                    break;
                case '$lte':
                    retValue = (_objAccessor(this.record, key) <= currCriteria['$lte']);
                    break;
                case '$gt':
                    retValue = (_objAccessor(this.record, key) > currCriteria['$gt']);
                    break;
                case '$gte':
                    retValue = (_objAccessor(this.record, key) >= currCriteria['$gte']);
                    break;
                case '$ne':
                    retValue = (_objAccessor(this.record, key) != currCriteria['$ne']);
                    break;
                case '$in':
                    var toMatch = _objAccessor(this.record, key);
                    for (var i = 0; i < currCriteria['$in'].length; i++){
                        if (currCriteria['$in'][i] == toMatch){
                            retValue = true;
                            break;
                        }
                    }
                    break;
                case '$nin':
                    var toMatch = _objAccessor(this.record, key);
                    for (var i = 0; i < currCriteria['$in'].length; i++){
                        if (currCriteria['$in'][i] == toMatch){
                            retValue = true;
                            break;
                        }
                    }
                    // reverse value since "not in"
                    retValue = !retValue;
                    break;
                default:
                    noMatch = true;
                    break;
            }
            if (!noMatch){
                keyCallback(retValue);
            }
        }
        if (currCriteriaLength > 1 || noMatch){
            return _doEvery(this.record, currCriteria, keyCallback);
        }
    }
    else{
        var retValue = (_objAccessor(this.record, key) == currCriteria);
        keyCallback(retValue);
    }
}

function _objAccessor(obj, objPath){
    objPath = objPath.split('.');
    for (var i = 0; i < objPath.length; i++){
        if (typeof obj[objPath[i]] != 'undefined'){
            obj = obj[objPath[i]];
        }
        else{
            return undefined;
        }
    }
    return obj;
}
