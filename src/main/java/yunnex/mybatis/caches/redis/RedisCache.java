/**
 * Copyright 2015-2018 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package yunnex.mybatis.caches.redis;

import org.apache.ibatis.cache.Cache;
import org.mybatis.caches.redis.DummyReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yunnex.redis.RedisCommands;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Cache adapter for Redis.
 *
 * @author Eduardo Macarron
 */
public final class RedisCache implements Cache {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCache.class);
    private final ReadWriteLock readWriteLock = new DummyReadWriteLock();
    private RedisCommands redisCommands;
    private String id;
    private Integer timeout;

    public RedisCache(final String id) {
        if (id == null) {
            throw new IllegalArgumentException("Cache instances require an ID");
        }
        this.id = id;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public void putObject(final Object key, final Object value) {
        try {
            redisCommands().hset(id, key.toString(), value);
            if (timeout != null) {
                redisCommands().expire(id, timeout, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            LOGGER.error("fail to putObject ,id:{},key:{},value:{},timeout:{}", id, key, value, timeout, e);
        }
    }

    @Override
    public Object getObject(final Object key) {
        try {
            return redisCommands().hgetObject(id, key.toString());
        } catch (Exception e) {
            LOGGER.error("fail to getObject ,id:{},key:{}", id, key, e);
        }
        return null;
    }

    @Override
    public Object removeObject(final Object key) {
        try {
            return redisCommands().hdel(id, key.toString());
        } catch (Exception e) {
            LOGGER.error("fail to removeObject ,id:{},key:{}", id, key, e);
        }
        return null;
    }

    @Override
    public void clear() {
        try {
            redisCommands().del(id);
        } catch (Exception e) {
            LOGGER.error("fail to clear ,id:{}", id, e);
        }
    }

    @Override
    public int getSize() {
        try {
            return redisCommands().hlen(id).intValue();
        } catch (Exception e) {
            LOGGER.error("fail to getSize,id:{},", id, e);
        }
        return 0;
    }

    @Override
    public ReadWriteLock getReadWriteLock() {
        return readWriteLock;
    }

    @Override
    public String toString() {
        return "Redis {" + id + "}";
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    private RedisCommands redisCommands() {
        if (redisCommands == null) {
            redisCommands = RedisCacheContext.getRedisCommands();
        }
        return redisCommands;
    }

}
