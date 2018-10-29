'use strict';

const async = require('async');
const redis = require('./redis');
const uuid = require('uuid');

const data = JSON.stringify({
    hello: 'world',
    this: 'is new world'
});

let startId = 1;

function get() {
    return startId++ + '-' + uuid.v4(); 
}

const maxCount = 200;
const maxList = new Array(maxCount);

function done(callback) {
    async.eachLimit(maxList, 10, function(item, callback) {
        setImmediate(function() {
            const key = get();
            async.series([
                function(callback) {
                    redis.set(key, data, function(err) {
                        if(err) {
                            console.error(err)
                            return callback(err);
                        }
                        redis.expire(key, 24 * 3600);
                        return callback(err);
                    });
                },
                function(callback) {
                    redis.hset('test-redis-hash', key, null, function(err) {
                        if(err) {
                            console.error(err);
                            redis.del(key);
                            return callback(err);
                        }
                        callback();
                    });
                },
                function(callback) {
                    redis.rpush('test-redis-mq', key, function(err) {
                        if(err) {
                            console.error(err);
                        }
                        callback();
                    });
                }
            ], function(err) {
                if(err) {
                    return callback(err);
                }
                callback();
            });
        });
    }, function(err) {
        if(err) {
            return callback(err);
        }
        callback();
    });
}

module.exports = done;
