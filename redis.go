package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/op/go-logging"
)

type RedisWrapper struct {
	redisPool *redis.Pool
	configure *Configure
	logger    *logging.Logger
}

func NewRedisWrapper(configure *Configure, logger *logging.Logger) *RedisWrapper {
	return &RedisWrapper{
		configure: configure,
		redisPool: &redis.Pool{
			MaxIdle:     64,
			IdleTimeout: 60 * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", configure.RedisAddress)
				if err != nil {
					return nil, err
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
		logger: logger,
	}
}

func (rw *RedisWrapper) GetFrequency(req *ParsedRequest, creatives InventoryCollection) (response []int, err error) {
	conn := rw.redisPool.Get()
	defer conn.Close()
	frIds := make([]interface{}, creatives.Len())
	for index, value := range creatives {
		frIds[index] = fmt.Sprintf("%v%v_%v",
			rw.configure.RedisFrequencyPrefix, req.Cid, value.AdId)
	}
	if response, err = redis.Ints(conn.Do("mget", frIds...)); err != nil {
		rw.logger.Warning("redis error: %v", err.Error())
	}
	return
}

func (rw *RedisWrapper) IncrFrequency(req *ParsedRequest, adId int) (err error) {
	conn := rw.redisPool.Get()
	defer conn.Close()

	frId := fmt.Sprintf("%v%v_%v",
		rw.configure.RedisFrequencyPrefix, req.Cid, adId)
	if _, err = conn.Do("incr", frId); err != nil {
		rw.logger.Warning("redis error: %v", err.Error())
	}
	return
}

func (rw *RedisWrapper) SaveRequest(req *ParsedRequest, creatives InventoryCollection, timeout int) error {
	conn := rw.redisPool.Get()
	defer conn.Close()
	req.Creatives = creatives
	if body, err := json.Marshal(*req); err != nil {
		return err
	} else if _, err := conn.Do("setex", rw.configure.RedisCachePrefix+req.Id, timeout, body); err != nil {
		rw.logger.Warning("redis error: %v", err.Error())
		return err
	}
	return nil
}

func (rw *RedisWrapper) GetRequest(id string) (*ParsedRequest, error) {
	conn := rw.redisPool.Get()
	defer conn.Close()
	if response, err := redis.Bytes(conn.Do("get", rw.configure.RedisCachePrefix+id)); err != nil {
		rw.logger.Warning("redis error: %v", err.Error())
		return nil, err
	} else {
		req := &ParsedRequest{}
		if err := json.Unmarshal(response, req); err != nil {
			return nil, err
		} else {
			return req, nil
		}
	}
}

func (rw *RedisWrapper) SetExpire(requestId string, expiredTime int) (err error) {
	conn := rw.redisPool.Get()
	defer conn.Close()
	if _, err = conn.Do("expire", "param:"+requestId, expiredTime); err != nil {
		rw.logger.Warning("redis error: %v", err.Error())
	}
	return
}
