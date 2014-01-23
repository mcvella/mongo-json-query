var vm = require('vm');
var async = require('async');
var _ = require('lodash');
var winston = require('winston');

/* CONSTANTS */
var LOGLEVEL = 'error';

var log = new ( winston.Logger )({ transports: [ new (winston.transports.Console)({ 'level': LOGLEVEL, 'timestamp': 1, 'colorize': 1 }) ] });


exports.find = function (records, criteria, callback)
{
    var boundFunc = _.bind(_tryMatch, { criteria : criteria } );

    async.filter( records, boundFunc, function( results ) {
        log.verbose( "End result", JSON.stringify(results, null, 4) );
        return callback(results);
    });


}

exports.match = function (record, criteria, callback )
{
    var boundFunc = _.bind(_tryMatch, { criteria : criteria } );

    boundFunc( record,  function( result ) {
        log.verbose( "End result", JSON.stringify(result, null, 4) );
        return callback(result);
    });
}



/* internal functions */

function _tryMatch( record, recordCallback )
{

    _doEvery( record, this.criteria, recordCallback);

}

function _doEvery( record, criteria, cb )
{
    log.verbose( "doEvery", JSON.stringify({ criteria : criteria, record : record }, null, 4) );

    var boundFunc = _.bind(_checkTrue, { record : record, criteria : criteria } );

    async.every( _.keys(criteria), boundFunc, function( result ) {
        return cb(result);
    });
}

function _callOr( record, criteria, cb )
{
    log.verbose( "callOr", JSON.stringify({ criteria : criteria, record : record }, null, 4) );

    var boundFunc = _.bind(_checkTrue, { record : record, criteria : criteria } );

    async.detect( _.keys(criteria), boundFunc, function( innerResult ) {
        log.verbose( "or", JSON.stringify(innerResult, null, 4) );
        return cb(innerResult);
    });
}

function _checkTrue( key, keyCallback )
{
    log.verbose("checkTrue", JSON.stringify(this.criteria, null, 4) );

    var currCriteria =  this.criteria[key];

    log.verbose( "checkTrue params", JSON.stringify({ key : key, currentCriteria : currCriteria, currentCriteriaType : typeof currCriteria }, null, 4) );

    if (
        ( key === '$and' )
            &&
            ( currCriteria instanceof Array )
        )
    {
        return _doEvery( this.record, currCriteria, keyCallback );
    }
    else if (
        ( key === '$or' )
            &&
            ( currCriteria instanceof Array )
        )
    {
        return _callOr( this.record, currCriteria, keyCallback );
    }
    else if ( key === '$where' )
    {
        var contextCode = '';
        var recordKeys = _.keys( this.record );

        for ( var k = 0; k < recordKeys.length; k++ )
        {
            contextCode += 'var ' + recordKeys[k] + "=" + JSON.stringify(this.record[recordKeys[k]]) + ';';
        }

        var retValue = false;
        try {
            retValue = vm.runInThisContext( contextCode + this.criteria[key] )
        }
        catch (e) {
            log.warn( "Error in $where clause: " + e);
        };

        keyCallback( retValue );

    }
    else if ( typeof currCriteria === 'object' )
    {
        if (
            ( _.keys( currCriteria ).length == 1 )
             &&
            ( _.keys( currCriteria )[0].match(/^\$(exists|lt|lte|gt|gte|ne|in|nin)$/))
         )
        {
            var retValue = false;

            switch (_.keys(currCriteria)[0])
            {
                case '$exists' :

                    var isDef = ( typeof _objAccessor(this.record, key) != 'undefined' );

                    retValue = currCriteria['$exists'] ? isDef : (!isDef);
                    break;

                case '$lt' :

                    retValue = ( _objAccessor(this.record, key) < currCriteria['$lt'] );
                    break;

                case '$lte' :

                    retValue = ( _objAccessor(this.record, key) <= currCriteria['$lte'] );
                    break;

                case '$gt' :

                    retValue = ( _objAccessor(this.record, key) > currCriteria['$gt'] );
                    break;

                case '$gte' :

                    retValue = ( _objAccessor(this.record, key) >= currCriteria['$gte'] );
                    break;

                case '$ne' :

                    retValue = ( _objAccessor(this.record, key) != currCriteria['$ne'] );
                    break;

                case '$in' :

                    var toMatch = _objAccessor(this.record, key);
                    for ( var i = 0; i < currCriteria['$in'].length; i++ )
                    {
                        if ( currCriteria['$in'][i] == toMatch )
                        {
                            retValue = true;
                            break;
                        }
                    }
                    break;

                case '$nin' :

                    var toMatch = _objAccessor(this.record, key);
                    for ( var i = 0; i < currCriteria['$in'].length; i++ )
                    {
                        if ( currCriteria['$in'][i] == toMatch )
                        {
                            retValue = true;
                            break;
                        }
                    }

                    // reverse value since "not in"
                    retValue = ! retValue;

                    break;

            }

            log.verbose( "check result " +  retValue );

             keyCallback( retValue );
        }
        else
        {
            return _doEvery( this.record, currCriteria, keyCallback );
        }
    }
    else
    {
        var retValue = ( _objAccessor( this.record, key ) == currCriteria );
        log.verbose( "check result " +  retValue );

         keyCallback( retValue );
    }

}

function _objAccessor( obj, objPath )
{
    log.verbose( "_objAccessor", JSON.stringify({ obj : JSON.stringify(obj, null, 4), path : objPath }, null, 4) )
    objPath = objPath.split(".");

    for(var i = 0; i < objPath.length; i++)
    {
        if ( typeof obj[ objPath[i] ] != 'undefined' )
        {
            obj = obj[ objPath[i] ];
        }
        else
        {
            return undefined;
        }
    }
    return obj;
}