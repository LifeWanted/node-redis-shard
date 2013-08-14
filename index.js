
var RedisShard  = require( './lib/RedisShard' );
var Config      = require( './lib/Configuration' );

exports.createClient = function( opts ){
    return new RedisShard( new Config( opts ) );
};
