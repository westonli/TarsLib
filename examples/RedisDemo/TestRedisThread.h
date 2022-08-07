#ifndef __TEST_REDIS_THREAD_H
#define __TEST_REDIS_THREAD_H

#include "servant/Application.h"
#include "tc_redis.h"

class RedisThread : public TC_Thread, public TC_ThreadLock
{
public:
	RedisThread(int second);

	virtual void run();

	void test_redis();

	void test_async_redis();

	void test_http();

	void test_redis_prx();

	void test_redis_get_prx();

	void test_redis_set();

	void test_async_redis_get();
private:
	int _second;
	bool _bTerminate;

	Communicator _comm;

	RedisPrx _redisPrx;
};
#endif
