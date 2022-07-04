#include "TestRedisThread.h"
#include <iostream>
#include "util/tc_custom_protocol.h"

class RedisCallBack : public ServantProxyCallback
{
public:
	int onDispatch(ReqMessagePtr msg)
	{
		if (msg->response->iRet != TARSSERVERSUCCESS)
		{
			return onDispatchException(msg->request, *msg->response);
		}

		string sRet(msg->response->sBuffer.begin(), msg->response->sBuffer.end());

		if (sRet == "AUTH_SUCC")
		{
			LOG_CONSOLE_DEBUG << "sRet:" << sRet << endl;
			return 0;
		}

		LOG_CONSOLE_DEBUG << endl;
		assert(msg->response->sBuffer.size() == sizeof(shared_ptr<RedisRsp>));

		shared_ptr<RedisRsp> rsp = *(shared_ptr<RedisRsp> *)(msg->response->sBuffer.data());

		LOG_CONSOLE_DEBUG << "rsp:" << rsp->getBuffer() << endl;

		return 0;
	}

	int onDispatchException(const RequestPacket &request, const ResponsePacket &response)
	{
		LOG_CONSOLE_DEBUG << endl;
		return 0;
	}


	void onConnect(const TC_Endpoint &ep)
	{
		LOG_CONSOLE_DEBUG << endl;
	}

	void onClose()
	{
		LOG_CONSOLE_DEBUG << endl;
	}
};
typedef tars::TC_AutoPtr<RedisCallBack> RedisCallBackPtr;


RedisThread::RedisThread(int second):_second(second), _bTerminate(false)
{
	ProxyProtocol prot;
	prot.requestFunc = RedisProxy::redisRequest;
    prot.responseFunc = RedisProxy::redisResponse;

	string sHost = "127.0.0.1";
	string sPasswd = "123456";
	int iPort = 12345;

    _redisPrx = _comm.stringToProxy<RedisPrx>(RedisProxy::genRedisObj(sHost, sPasswd, iPort));
    _redisPrx->tars_set_protocol(prot, 3);
}

void RedisThread::test_redis_set()
{	
	string sKey = "aaa1";
	string sValue(1000000, 'c');

	int iRet = _redisPrx->set(sKey, sValue);
	LOG_CONSOLE_DEBUG << "iRet:" << iRet << " sKey:" << sKey << " sValue size:" << sValue.size() << endl;
}

void RedisThread::test_redis_prx()
{	
	string sKey = "aaa1";
	string sValue;
	int iRet = _redisPrx->get(sKey, sValue);
	LOG_CONSOLE_DEBUG << "iRet:" << iRet << " sKey:" << sKey << " sValue size:" << sValue.size() << endl;
}

void RedisThread::test_redis_get_prx()
{	
	vector<string> vKey;
	vKey.push_back("xxxxx");
	vKey.push_back("aaa");
	vKey.push_back("aaa1");
	vKey.push_back("aaa12");

	map<string,string> mValue;
	vector<string> vNoKey;

	int iRet = _redisPrx->get(vKey, mValue, vNoKey);

	LOG_CONSOLE_DEBUG << "iRet:" << iRet 
					<< " vKey:" << TC_Common::tostr(vKey) 
					<< " vNoKey:" << TC_Common::tostr(vNoKey) 
					<< endl;
}

void RedisThread::run(void)
{
	int count = 0;

	while(!_bTerminate)
	{
		{
			try
			{
				test_redis_prx();

				test_redis_get_prx();

				test_redis_set();
			}
			catch(TarsException& e)
			{     
				cout << "TarsException: " << e.what() << endl;
			}
			catch(...)
			{
				cout << "unknown exception" << endl;
			}
		}

		if(count++>10000)
		{
			_bTerminate = true;
			break;
		}

		// {
        //     TC_ThreadLock::Lock sync(*this);
        //     timedWait(1*1);
		// }
	}
}
