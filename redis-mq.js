'use strict';

const redis = require('./redis');
const CronJob = require('cron').CronJob;
const async = require('async');
const pullMessgae = require('./redis-pop');

const MQ_NAME = 'test-redis-mq';
const MQ_HASH_NAME = 'test-redis-hash';
const MQ_HASH_RETRY_TIMES = 'test-redis-retry-hash';

let MQ_FETCH_STATUS = false;

const maxAckTime = 8 * 1000; // 8s 最长消费时间
const maxAckTimeout = 60 * 1000; // 消费超时时间 

// const retryTime = 20; // 重试时间 20s 
const failureRate = 0.5; // 失败率
// const successRate = 0.4; // 成功率
const timeoutRate = 0.1; // 超时率
const eachMessageCount = 500; // 每次 Message 获取数量
const maxRetryTimes = 5; // 最大重试次数
// const maxRedisMessageCount = 500;
const minRedisMessageCount = 400;
// const eachPullCount = 200;

function messageCount(callback) {
    callback = callback || function(err, count) {
        if(err) {
            console.error(`[获取 Redis 长度] ERROR: ${err.message}`);
        } else {
            console.log(`[获取 Redis 长度] 当前队列长度为: ${count}`);
        }
    }
    return redis.llen(MQ_NAME, function(err, count) {
        if(err) {
            return callback(err);
        }
        callback(undefined, count);
    });
}

function returnRate() {
    return 'success';
    const tmp = Math.random();
    console.log(`随机 tmp: ${tmp}`);
    if(tmp < timeoutRate) {
        return 'timeout';
    } else if(tmp - timeoutRate < failureRate) {
        return 'failure';
    } else {
        return 'success';
    }
}

function pullMessageFromKafuka(callback) {
    if(MQ_FETCH_STATUS) {
        console.log(`[fetch message] 但是已经 fetch 被堵塞`);
        setTimeout(function() {
            callback();
        });
    }
    console.log(`[fetch message] 开始 fetch message`);
    MQ_FETCH_STATUS = true;
    pullMessgae(function(err) {
        MQ_FETCH_STATUS = false;
        if(err) {

            console.log(`[fetch message] fetch message 失败 error: ${err.message}`);
            return callback(err);
        }
        console.log(`[fetch message] fetch message 结束`);
        messageCount(function(err, count) {
            console.log(`[fetch message] 最后数量为: ${count}`);
            callback();
        });
    });
}

function fetch(count, callback) {
    let mqCount = 0;
    if(count < 0) {
        count = 1;
    }
    async.series([
        function(callback) {
            // 检查 redis 的队列长度
            messageCount(function(err, count) {
                if(err) {
                    return callback(err);
                }
                console.log(`[获取 Redis 长度] 当前队列长度为: ${count}`);
                if(count > 0) {
                    mqCount = count;
                }
                return callback();
            });
        },
        function(callback) {
            console.log(`[检查 Redis 数量] 当前数量为: ${mqCount}, min 数量: ${minRedisMessageCount}`);
            if(mqCount > minRedisMessageCount) {
                console.log(`[检查 Redis 数量] 发现数量已经足够`);
                return callback();
            }
            // 执行获取消息
            console.log(`[检查 Redis 数量] 发现数量不足，需要重新获取`);
            pullMessageFromKafuka(callback);
        },
        function(callback) {
            console.log(`[fetch message key] 开始获取消息数量为: ${count}`);
            const array = new Array(count);
            const result = [];

            async.eachLimit(array, 10, function(_a, callback) {
                redis.lpop(MQ_NAME, function(err, key) {
                    if(err) {
                        console.log(`[fetch message key] 获取 key 失败 ${err.message}`);
                        return callback();
                    }
                    console.log(`[fetch message key] 成功 key: ${key}`) ;
                    redis.hset(MQ_HASH_NAME, key, Date.now(), function(err) {
                        console.log(`[redis hash set] 设置 hash id: ${key} 相关时间`);
                        if(err) {
                            console.log(`[redis hash set] 设置 hash id: ${key} 相关时间失败`);
                            console.log(`[redis hash set] 重新 push 消息`);
                            redis.lpush(MQ_NAME, key);
                            return callback(err);
                        }
                        console.log(`[fetch message]开始 fetch message`)
                        redis.get(key, function(err, _result) {
                            if(err) {
                                console.log(`[fetch message] fetch message 失败, key: ${key} error: ${err.messahe}`);
                                redis.rpush(MQ_NAME, key);
                                redis.hset(MQ_HASH_NAME, key, null);
                                return callback(err);
                            }
                            console.log(`[fetch message] fetch message id: ${key} 成功，消息体为: ${_result}`);
                            result.push({
                                key: key,
                                data: _result
                            });
                            callback();
                        });
                    });
                    
                });
            }, function(err) {
                if(err) {
                    return callback(err);
                }
                callback(undefined, result);
            });
        }
    ], function(err, result) {
        if(err) {
            return callback(err);
        }
        callback(undefined, result[result.length - 1]);
    });
}

// 检查超时的消息
// 重置已经过

// fetch(20, function(err, result) {
//     if(err) {
//         console.error(err);
//     }
//     console.log('获取消息成功')
//     console.log(result);
// });

function incrFailureTimes(key, callback) {
    console.log(`[retry times] 添加 & 查询 key: ${key}`);
    redis.hincrby(MQ_HASH_RETRY_TIMES, key, 1, function(err, count) {
        console.log(`[retry times] 查询到 key: ${key} 重试之后的次数为: ${count}`);
        if(err) {
            return callback(err);
        }
        if(count > maxRetryTimes) {
            console.log(`[retry times] key: ${key} 超时次数为 ${count} 超过预定次数 ${maxRetryTimes}`);
            redis.hdel(MQ_HASH_NAME, key);
            return callback();
            // todo 保存到 MY_SQL
        } else {
            console.log(`[retry times] 将 key: ${key} 重新加入到队列，并清除原来的时间计数 `);
            redis.rpush(MQ_NAME, key, function(err) {
                if(err) {
                    return callback();
                }

                console.log(`[retry times] key: ${key} 重新加入队列成功 `);
                redis.hset(MQ_HASH_NAME, key, null, function(err) {
                    console.log(`[retry times] key: ${key} 重新初始化时间成功 `);
                    callback();
                });
            });
        }
    });
}

let checkStatus = true;

function checkExpireMessage() {
// 1. list 有 hash null => 未消费
// 2. list 有 hash now => 重试消费
// 3. list 无 hash null => 数据丢失 => 重新 push 数据到 list
// 4. list 无 hash now => 消费中 or 消费超时

// 先 list 后 hash
// 如果 3:
// ​	检查 hash 是否存在，如果存在，那就是铁定的数据丢失，重新导入数据
// 如果 4:
// ​	先检查 hash 是否存在，如果存在，就计算时间是否超时，如果超时就记一次超时，重新加入到队列
    let mgCount;
    let mqMessages;
    let hashMap;

    if(checkStatus) {
        checkStatus = false;
    } else {
        return;
    }

    console.log(`[message check] 开始检查消息过期问题`);
    async.series([
        function(callback) {
            console.log(`[message check]  检查队列长度`);
            messageCount(function(err, count) {
                if(err) {
                    return callback(err);
                }
                mgCount = count;
                console.log(`[message check]  队列长度为: ${count}`);
                callback();
            });
        },
        function(callback) {
            const count = mgCount + 200
            console.log(`[message check] 加上默认 pull 数量， 获取总数为: ${count}`);
            redis.lrange(MQ_NAME, 0, count, function(err, data) {
                if(err) {
                    return callback(err);
                }
                mqMessages = data;
                console.log(`[message check] 获取消息的数量为: ${mqMessages.length}`);
                callback();
            });
        },
        function(callback) {
            console.log(`[message check] 准备获取 HASH MAP`);
            redis.hgetall(MQ_HASH_NAME, function(err, data) {
                if(err) {
                    return callback(err);
                }
                hashMap = data;
                callback();
            });
        }
    ], function(err) {
        if(err) {
            console.error(`[message check] 检查消息失败: ${err.message}`);
            return;
        }

        // console.log(mqMessages);
        // console.log(hashMap);

        const missingList = [];
        const timeoutList = [];

        const keys = Object.keys(hashMap);

        console.log(`[message check] 消息 HASH 内的消息总数量为 ${keys.length}`);

        for(const key of keys) {
            const index = mqMessages.indexOf(key)
            if(index < 0 && !hashMap[key]) {
                console.log(index, hashMap[key]);
                // 数据缺失
                console.log(`[message check] 检查发现 ${key} 这个数据缺失，需要重新 push 到 mq`)
                // redis.rpush(key);
                missingList.push(key);
            } else if(index < 0 && hashMap[key] && (Date.now() - hashMap[key]) > maxAckTimeout) {
                // 数据 ack 超时
                console.log(`[message check] 检查发现 ${key} 超时， 需要重新 push 到 mq，并及加次数`);
                timeoutList.push(key);
            }
        }

        console.log(`[message check] 最后数据缺失的 list 长度为: ${missingList.length}`);
        console.log(`[message check] 最后数据超时的 list 长度为: ${timeoutList.length}`);

        async.series([
            function(callback) {
                console.log(`[message check] 修复 数据缺失`);
                async.eachLimit(missingList, 10, function(key, callback) {
                    redis.hexists(MQ_HASH_NAME, key, function(err, data) {
                        if(err) {
                            console.log(err);
                            return callback();
                        }
                        if(!data) {
                            console.log(`[message check] 确定数据是因为延迟问题`);
                            return callback();
                        }
                        redis.rpush(MQ_NAME, key, function(err) {
                            if(err) {
                                console.log(err);
                            }
                            callback();
                        });
                    });
                    
                }, callback);
            },
            function(callback) {
                console.log(`[messsage check] 修复数据超时问题`);
                async.eachLimit(timeoutList, 10, function(key, callback) {
                    // 重新确认
                    redis.hget(MQ_HASH_NAME, key, function(err, data) {
                        if(err) {
                            console.log(err);
                            return callback();
                        }
                        if(!data) {
                            console.log(`[message check] 确定数据是因为延迟问题`);
                            return callback();
                        }
                        incrFailureTimes(key, function(err) {
                            if(err) {
                                console.log(err);
                            }
                            callback();
                        });
                    });
                }, callback);
            }
        ], function(err) {
            console.log(`[message check] 修复数据成功`);
            checkStatus = true;
        })
    });
}

function ackMessage(key, success, callback) {
    console.log(`[ack message] key 为 ${key} 且状态为: ${success ? 'success' : 'false'}`);
    async.series([
        function(callback) {
            redis.hget(MQ_HASH_NAME, key, function(err, time) {
                if(!time) {
                    console.log(`[ack message] key: ${key} 已经过期了`);
                    return callback(new Error('key 已被过期消费'));
                }
                if(success) {
                    console.log(`[ack message] 消费成功删除 key: ${key}`);
                    redis.hdel(MQ_HASH_NAME, key, function(err) {
                        if(err) {
                            return callback(err);
                        }
                        redis.hdel(MQ_HASH_RETRY_TIMES, key);
                        redis.del(key);
                        callback();
                    });
                } else {
                    console.log(`[ack message] 消费失败重试 key: ${key}`);

                    incrFailureTimes(key, function(err) {
                        if(err) {
                            console.log(err);
                        }
                        callback();
                    });
                }
                
            });
        }
    ], function(err) {
        if(err) {
            return callback(err);
        }
        callback();
    });
};

function clientDone() {
    console.log(`[client] 我是一个快乐的客户端`);

    let datas;

    async.series([
        function(callback) {
            console.log(`[client] fetch ${eachMessageCount} 条数据`);
            fetch(eachMessageCount, function(err, result) {
                if(err) {
                    return callback(err);
                }
                datas = result;
                console.log(`[client] 获得数据的数量为: ${datas.length}`);
                return callback();
            });
        },
        function(callback) {
            async.eachSeries(datas, function(_data, callback) {
                const { key, data } = _data;
                console.log(`[client] 开始处理数据 key: ${key}` );

                const res = returnRate();

                if(res === 'failure') {
                    console.log(`[client] 抽到了失败 key: ${key}` );
                    setTimeout(() => {
                        ackMessage(key, false, callback);
                    }, 100);
                } else if (res === 'success') {
                    console.log(`[client] 抽到了成功 key: ${key}` );
                    setTimeout(() => {
                        ackMessage(key, true, callback);
                    }, 100);
                } else if(res === 'timeout') {
                    console.log(`[client] 抽到了超时 key: ${key}` );
                    setTimeout(() => {
                        callback();
                    }, 100);
                } else {
                    throw new Error(`抽到了奇怪的东西: ${res}`);
                }
            }, function() {
                console.log(`[client] 所有消息都处理完了, 棒棒哒💯`);
                callback();
            });
        },
    ], function() {
        console.log(`[client] 快乐的客户端完成了一次使命`);
        console.log(`[client] 重新开始新的使命`);

        setTimeout(() => {
            clientDone();
        }, 100);
    })
}

clientDone();

// new CronJob('*/6 * * * * *', function() {
//     console.log('=================== 快乐的修复数据 ====================')
//     checkExpireMessage();
// }, null, true);