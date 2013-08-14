
module.exports = (function(){
    function Configuration( opts ){
        this.servers = opts.servers || [];
    }

    return Configuration;
})();
