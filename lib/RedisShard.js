
var redis       = require( 'redis' );
var async       = require( 'async' );
var HashRing    = require( './HashRing' );
var Pipeline    = require( './Pipeline' );

module.exports = (function(){
    const FIND_HASH = /.*\{(.*)\}.*/;

    // Shardable commands have the exact same interface as their vanilla redis counterparts.
    const SHARDABLE_COMMANDS = [
        "append",           "bitcount",         "blpop",            "brpop",
        "debug object",     "decr",             "decrby",           "del",
        "dump",             "exists",           "expire",           "expireat",
        "get",              "getbit",           "getrange",         "getset",
        "hdel",             "hexists",          "hget",             "hgetall",
        "hincrby",          "hincrbyfloat",     "hkeys",            "hlen",
        "hmget",            "hmset",            "hset",             "hsetnx",
        "hvals",            "incr",             "incrby",           "incrbyfloat",
        "lindex",           "linsert",          "llen",             "lpop",
        "lpush",            "lpushx",           "lrange",           "lrem",
        "lset",             "ltrim",            "mget",             "move",
        "persist",          "pexpire",          "pexpireat",        "psetex",
        "pttl",             "rename",           "renamenx",         "restore",
        "rpop",             "rpush",            "rpushx",           "sadd",
        "scard",            "sdiff",            "set",              "setbit",
        "setex",            "setnx",            "setrange",         "sinter",
        "sismember",        "smembers",         "sort",             "spop",
        "srandmember",      "srem",             "strlen",           "sunion",
        "ttl",              "type",             "watch",            "zadd",
        "zcard",            "zcount",           "zincrby",          "zrange",
        "zrangebyscore",    "zrank",            "zrem",             "zremrangebyrank",
        "zremrangebyscore", "zrevrange",        "zrevrangebyscore", "zrevrank",
        "zscore"
    ];

    // Split shardable commands work with the exact same interface as their vanilla counterparts,
    // just like SHARDABLE_COMMANDS, but also accept an array of argument arrays. In the latter form
    // each argument array will be sent to one server in the order in which they were added.
    const SPLIT_SHARDABLE_COMMANDS = [
        "auth",             "select"
    ];

    // Unshardable commands are not supported by this module and will throw an error if they are
    // called.
    const UNSHARDABLE_COMMANDS = [
        "bgrewriteaof",     "bgsave",           "bitop",            "brpoplpush",
        "client kill",      "client list",      "client getname",   "client setname",
        "config get",       "config set",       "config resetstat", "dbsize",
        "debug segfault",   "discard",          "echo",             "eval",
        "evalsha",          "exec",             "flushall",         "flushdb",
        "info",             "keys",             "lastsave",         "migrate",
        "monitor",          "mset",             "msetnx",           "multi",
        "object",           "ping",             "psubscribe",       "publish",
        "punsubscribe",     "quit",             "randomkey",        "rpoplpush",
        "save",             "script exists",    "script flush",     "script kill",
        "script load",      "sdiffstore",       "shutdown",         "sinterstore",
        "slaveof",          "slowlog",          "smove",            "subscribe",
        "sunionstore",      "sync",             "time",             "unsubscribe",
        "unwatch",          "zinterstore",      "zunionstore"
    ];

    /// function( {Configuration} config )
    ///
    ///
    function RedisShard( config ){
        this.config         = config;
        this.nodes          = [];
        this.connections    = {};
        for( var i in config.servers ){
            var server = config.servers[ i ];
            var client = redis.createClient(
                server.port || null,
                server.host || null,
                server
            );
            this.connections[ server.name ] = client;
            this.nodes.push( server.name );
        }

        this.ring = new HashRing( this.nodes );
    }

    RedisShard.SHARDABLE_COMMANDS   = SHARDABLE_COMMANDS;
    RedisShard.UNSHARDABLE_COMMANDS = UNSHARDABLE_COMMANDS;

    var RedisShardProto = RedisShard.prototype;

    RedisShardProto.getServerName = function( key ){
        var group   = FIND_HASH.exec( key );
        if( group && group[ 1 ] ){
            key = group[ 1 ];
        }
        return this.ring.getNode( key );
    };

    RedisShardProto.getServer = function( key ){
        var name = this.getServerName( key );
        return this.connections[ name ];
    };

    RedisShardProto.pipeline = function(){
        return new Pipeline( this );
    };

    RedisShardProto.end = function(){
        for( var i = 0; i < this.nodes.length; ++i ){
            this.connections[ this.nodes[ i ] ].end();
        }
    };

    function makeShardableMethod( command ){
        return function( key ){
            var client = this.getServer( key );
            return client[ command ].apply( client, arguments );
        };
    }

    function makeSplitShardableMethod( command ){
        return function(){
            var i, conn;
            var args        = Array.prototype.slice.call( arguments );
            var callback    = args.pop();
            var calls       = {};
            if( Array.isArray( args[ 0 ] ) && Array.isArray( args[ 0 ][ 0 ] ) ){
                for( i = 0; i < this.nodes.length; ++i ){
                    conn = this.connections[ this.nodes[ i ] ];
                    calls[ this.nodes[ i ] ] = conn[ command ].bind( conn, args[ i ] );
                }
            }
            else {
                for( i = 0; i < this.nodes.length; ++i ){
                    conn = this.connections[ this.nodes[ i ] ];
                    calls[ this.nodes[ i ] ] = conn[ command ].bind( conn, args );
                }
            }
            async.parallel( calls, callback );
        };
    }

    function makeUnshardableMethod( command ){
        return function(){
            throw new Error( command + ' is not a shardable command.' );
        };
    }

    var i, command;
    for( i in SHARDABLE_COMMANDS ){
        command = SHARDABLE_COMMANDS[ i ];
        RedisShardProto[ command ] = makeShardableMethod( command );
    }
    for( i in SPLIT_SHARDABLE_COMMANDS ){
        command = SPLIT_SHARDABLE_COMMANDS[ i ];
        RedisShardProto[ command ] = makeSplitShardableMethod( command );
    }
    for( i in UNSHARDABLE_COMMANDS ){
        command = UNSHARDABLE_COMMANDS[ i ];
        RedisShardProto[ command ] = makeUnshardableMethod( command );
    }

    return RedisShard;
})();
