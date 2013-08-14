
var redis       = require( 'redis' );
var HashRing    = require( './HashRing' );
var Pipeline    = require( './Pipeline' );

module.exports = (function(){
    const FIND_HASH = /.*\{(.*)\}.*/;

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

    const UNSHARDABLE_COMMANDS = [
        "auth",             "bgrewriteaof",     "bgsave",           "bitop",
        "brpoplpush",       "client kill",      "client list",      "client getname",
        "client setname",   "config get",       "config set",       "config resetstat",
        "dbsize",           "debug segfault",   "discard",          "echo",
        "eval",             "evalsha",          "exec",             "flushall",
        "flushdb",          "info",             "keys",             "lastsave",
        "migrate",          "monitor",          "mset",             "msetnx",
        "multi",            "object",           "ping",             "psubscribe",
        "publish",          "punsubscribe",     "quit",             "randomkey",
        "rpoplpush",        "save",             "script exists",    "script flush",
        "script kill",      "script load",      "sdiffstore",       "select",
        "shutdown",         "sinterstore",      "slaveof",          "slowlog",
        "smove",            "subscribe",        "sunionstore",      "sync",
        "time",             "unsubscribe",      "unwatch",          "zinterstore",
        "zunionstore"
    ];

    /// function( {Configuration} config )
    ///
    ///
    function RedisShard( config ){
        this.config         = config;
        this.nodes          = [];
        this.connections    = {};
        for( var server in config.servers ){
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

    function makeShardableMethod( command ){
        return function( key ){
            var client = this.getServer( key );
            return client[ command ].apply( client, arguments );
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
    for( i in UNSHARDABLE_COMMANDS ){
        command = UNSHARDABLE_COMMANDS[ i ];
        RedisShardProto[ command ] = makeUnshardableMethod( command );
    }

    return RedisShard;
})();
