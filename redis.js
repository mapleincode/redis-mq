
'use strict';

const Redis = require('ioredis');
const redis = new Redis(6380, '192.168.2.110', {
    db: 1
});

module.exports = redis;
