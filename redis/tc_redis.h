#ifndef tc_redis_h__
#define tc_redis_h__
#include "util/tc_ex.h"
#include "tup/TarsType.h"
#include <vector>
#include <map>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <cfloat>
#include "util/tc_epoller.h"
#include "util/tc_socket.h"
#include "util/tc_clientsocket.h"
#include "servant/ServantProxy.h"
#include "util/tc_custom_protocol.h"
#include "util/tc_thread_rwlock.h"

using namespace std;

namespace tars
{

/////////////////////////////////////////////////
/** 
* @file  tc_redis.h 
* @brief redis操作类. 
*  
* @author  
*/           
/////////////////////////////////////////////////
/**
* @brief 异常类
*/
struct TC_Redis_Exception : public TC_Exception
{
    TC_Redis_Exception(const string &sBuffer) : TC_Exception(sBuffer){};
    ~TC_Redis_Exception() throw(){};    
};

/**
* @brief Redis配置接口
*/
struct TC_RDConf
{
    /**
    * 主机地址
    */
    string _host;

    /**
    * 密码
    */
    string _password;

    /**
    * 端口
    */
    int _port;

    /*
     * redis数据库索引，默认使用0号数据库
     */
    int _index;

    /**
    * @brief 构造函数
    */
    TC_RDConf()
        : _port(0)
        , _index(0)
    {
    }

    /**
    * @brief 读取配置. 
    * 
    * @param mpParam 存放配置的map 
    *        host: 主机地址
    *        pass:密码
    *        port:端口
    */
    void loadFromMap(const map<string, string> &mpParam)
    {
        map<string, string> mpTmp = mpParam;

        _host        = mpTmp["host"];
        _password    = mpTmp["pass"];
        _port        = atoi(mpTmp["port"].c_str());

        if (mpTmp["port"] == "")
        {
            _port = 6379;
        }

        if (mpTmp["index"] != "")
        {
            _index = atoi(mpTmp["index"].c_str());
        }
    }
};

class RedisReq: public TC_CustomProtoReq
{
};

class RedisRsp: public TC_CustomProtoRsp
{
public:
    bool parseType(const string& buffer, size_t& iStartPos)
    {
        bool bRet = false;

        string sSep = "\r\n";
        char f = buffer[iStartPos];
        size_t iPos = _buffer.find(sSep, iStartPos);

        switch (f)
        {
        case '+':
        case '-':
        case ':':
            if (iPos + sSep.size() <= _buffer.size())
            {
                bRet = true;
                iStartPos = iPos + sSep.size();
            }
            break;
        case '$':
            if (iPos != string::npos)
            {
                int iLen = TC_Common::strto<size_t>(_buffer.substr(1+iStartPos, iPos));

                if (iLen >= 0 && (iLen + iPos + sSep.size()*2 <= _buffer.size()))
                {
                    bRet = true;
                    iStartPos = iLen + iPos + sSep.size()*2;
                }
                else if(iLen == -1 && (iPos + sSep.size()) <= _buffer.size())
                {
                    bRet = true;
                    iStartPos = iPos + sSep.size();
                }
            }
            break;
        default:
            bRet = false;
            break;
        }
		return bRet;
    }

	virtual bool decode(TC_NetWorkBuffer::Buffer &data)
	{
        bool bRet = true;

        _buffer.append(data.buffer(), data.length());

        string sSep = "\r\n";
        string sData;
        size_t iPos;
        
        if (_buffer.empty())
        {
            return false;
        }

        char f = _buffer[0];
        iPos = _buffer.find(sSep);

        switch (f)
        {
        case '*':
            if (iPos != string::npos)
            {
                size_t iSize = TC_Common::strto<size_t>(_buffer.substr(1, iPos));
                size_t iStartPos = iPos + sSep.size();

                for (size_t i = 0; i < iSize; i++)
                {
                    if (!parseType(_buffer, iStartPos))
                    {
                        bRet = false;
                        break;
                    }
                }
            }
            break;
        default:
            size_t iStartPos = 0;
            bRet = parseType(_buffer, iStartPos);
            break;
        }

		data.clear();
		return bRet;
	}
};


class TC_Redis_Config_Holder : public  tars::TC_HandleBase, public tars::TC_Singleton<TC_Redis_Config_Holder>
{
public:
	/**
    * @brief 构造函数
    */
	TC_Redis_Config_Holder(){}

	/**
    * @brief 析构函数.
    */
	~TC_Redis_Config_Holder(){}

    void set_password(const string& sObj, const string& sPasswd)
    {
        TC_ThreadWLock w(_rwl);
        _mObjPasswd[sObj] = sPasswd;
    }

    int get_password(const string& sObj, string& sPasswd)
    {
        TC_ThreadRLock w(_rwl);

        map<string, string>::iterator it = _mObjPasswd.find(sObj);
        
        if (it != _mObjPasswd.end())
        {
            sPasswd = it->second;

            return 0;
        }

        return -1;
    }

protected:
	/**
    * @brief copy contructor，只申明,不定义,保证不被使用
    */
	TC_Redis_Config_Holder(const TC_Redis_Config_Holder &);

	/**
    * @brief 只申明,不定义,保证不被使用
    */
	TC_Redis_Config_Holder &operator=(const TC_Redis_Config_Holder &);

private:
	TC_ThreadRWLocker _rwl;
    map<string, string> _mObjPasswd;
};

class RedisProxy: public ServantProxy
{
public:
    static string genRedisObj(const string& sHost, const string& sPasswd, const int& port)
    {
        string sObj = "TARS.RedisServer.RedisObj." + sHost + "." + TC_Common::tostr(port);
        TC_Redis_Config_Holder::getInstance()->set_password(sObj, sPasswd);

        sObj += "@tcp -h " + sHost + " -p " + TC_Common::tostr(port);
        if (!sPasswd.empty())
        {
            sObj += " -e 1";
        }

        return sObj;
    }

    static string genRedisObj(const TC_RDConf& tcRDConf)
    {
        string sObj = "TARS.RedisServer.RedisObj." + tcRDConf._host + "." + TC_Common::tostr(tcRDConf._port);
        sObj += "@tcp -h " + tcRDConf._host + " -p " + TC_Common::tostr(tcRDConf._port);

        if (!tcRDConf._password.empty())
        {
            sObj += " -e 1";
            TC_Redis_Config_Holder::getInstance()->set_password(sObj, tcRDConf._password);
        }

        return sObj;
    }
    
    static shared_ptr<TC_NetWorkBuffer::Buffer> redisRequest(tars::RequestPacket& request, TC_Transceiver *trans)
    {
        shared_ptr<TC_NetWorkBuffer::Buffer> buff = std::make_shared<TC_NetWorkBuffer::Buffer>();

        if (TC_Port::strncasecmp(request.sFuncName.c_str(), "InnerAuthServer", request.sFuncName.size()) == 0)
        {
            string sPasswd;
            TC_Redis_Config_Holder::getInstance()->get_password(request.sServantName, sPasswd);
            if (!sPasswd.empty())
            {
                buff->addBuffer("Auth " + sPasswd + "\r\n");
            }
        }
        else
        {
            shared_ptr<RedisReq> &data = *(shared_ptr<RedisReq>*)request.sBuffer.data();
            data->encode(buff);
            data.reset();
        }

        return buff;
    }

    static TC_NetWorkBuffer::PACKET_TYPE redisResponse(TC_NetWorkBuffer &in, ResponsePacket& rsp)
    {
        shared_ptr<RedisRsp> *context = (shared_ptr<RedisRsp>*)(in.getContextData());

        const int kAuthType = 0x40;

        if(!context)
        {
            context = new shared_ptr<RedisRsp>();
            *context = std::make_shared<RedisRsp>();
            in.setContextData(context, [](TC_NetWorkBuffer*nb){ shared_ptr<RedisRsp> *p = (shared_ptr<RedisRsp>*)(nb->getContextData()); if(p) { nb->setContextData(NULL); delete p; }});
        }

        if((*context)->incrementDecode(in))
        {	
            auto sBuf = in.getBuffer();
            string buffer;
            buffer.assign((*sBuf.get()).buffer(), (*sBuf.get()).length());

            (*sBuf.get()).clear();

            rsp.sBuffer.resize(sizeof(shared_ptr<RedisRsp>));

            shared_ptr<RedisRsp> &data = *(shared_ptr<RedisRsp>*)rsp.sBuffer.data();
            data = *context;

            auto ret = TC_NetWorkBuffer::PACKET_FULL;

            (*context) = NULL;
            delete context;
            in.setContextData(NULL);
            
            if (rsp.iMessageType == kAuthType)
            {
                string sRet = "AUTH_SUCC";
                rsp.sBuffer.assign(sRet.begin(), sRet.end());
            }

            return ret;
        }

        return TC_NetWorkBuffer::PACKET_LESS;
    }
    
    /**
    * @brief 初始化. 
    *  
    * @param sHost        主机IP
    * @param sPasswd      密码
    * @param port         端口
    * @return 无
    */
    void init(const string& sHost, const string& sPasswd = "", int port = 0)
    {
        _rdConf._host     = sHost;
        _rdConf._password = sPasswd;
        _rdConf._port     = port;
    }

    /**
    * @brief 初始化. 
    *  
    * @param tcDBConf 数据库配置
    */
    void init(const TC_RDConf& tcRDConf)
    {
        _rdConf = tcRDConf;
    }
    
    /**
    * @brief get数据 
    *  
    * @param sKey        
    * @param sValue      
    * @return 0 成功 1 没有数据 -1 失败
    */
    int get(const string& sKey, string& sValue)
    {
        int iRet = -1;

        string sCommand;
        vector<string> vPart;

        vPart.push_back("GET");
        vPart.push_back(sKey);

        buildCommand(vPart, sCommand);
        
        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            if (vBuffer[0].first == -1)
            {
                iRet = 1;
            }
            else
            {
                sValue = vBuffer[0].second;
                iRet = 0;
            }
        }
        
        return iRet;
    }

    /**
    * @brief get数据 
    *  
    * @param sKey        
    * @param vValue      
    * @return 0 成功 1 没有数据 -1 失败
    */
    int get(const string& sKey, vector<char>& vValue)
    {
        string sValue;
        int iRet = get(sKey, sValue);
        vValue.assign(sValue.begin(), sValue.end());

        return iRet;
    }

    /**
    * @brief set
    *  
    * @param sKey        
    * @param sValue      
    * @param expir 数据的过期时间   
    * @return 0 成功 -1 失败
    */
    int set(const string& sKey, const string& sValue, unsigned int expir = 0)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        if(expir == 0)
        {
            vPart.push_back("SET");
            vPart.push_back(sKey);
            vPart.push_back(sValue);
        }
        else
        {
            vPart.push_back("SETEX");
            vPart.push_back(sKey);
            vPart.push_back(TC_Common::tostr(expir));
            vPart.push_back(sValue);
        }

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = 0;
        }
        else
        {
            iRet = -1;
        }

        return iRet;
    }

    /**
    * @brief mset
    *  
    * @param vKeyValue        
    * @return 0 成功 -1 失败
    */
    int mset(const vector<pair<string, string> >& vKeyValue)
    {
        int iRet = -1;

        string sCommand;

        vector<string> vPart;

        vPart.push_back("MSET");

        for (size_t i = 0; i < vKeyValue.size(); i++)
        {
            vPart.push_back(vKeyValue[i].first);
            vPart.push_back(vKeyValue[i].second);
        }
        
        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = 0;
        }
        else
        {
            iRet = -1;
        }

        return iRet;
    }

    /**
    * @brief del
    *  
    * @param sKey        
    * @return 0 成功 -1 失败
    */
    int del( const string& sKey )
    {
        int iRet = -1;

        string sCommand;

        vector<string> vPart;

        vPart.push_back("DEL");
        vPart.push_back(sKey);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief exists
    *  
    * @param sKey        
    * @return 0-不存在；1-存在
    */
    int exists(const string& sKey)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("EXISTS");
        vPart.push_back(sKey);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief del
    *  
    * @param vKey        
    * @return 
    */
    int del(const vector<string>& vKey)
    {
        int iRet = -1;

        string sCommand;

        vector<string> vPart;

        vPart.push_back("DEL");

        for (size_t i = 0; i < vKey.size(); i++)
        {
            vPart.push_back(vKey[i]);
        }
        
        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    *@brief 批量获取数据
    *@param vKey
    *@param mpValues:返回key value对集合
    *@param vNoKey: 不存在的key集合
    * @return 0 成功 -1 失败
    */   
    int get(const vector<string>& vKey, map<string,string>& mValues, vector<string>& vNoKey)
    {
        int iRet = -1;

        string sCommand;

        vector<string> vPart;
        vPart.push_back("MGET");

        for (size_t i=0; i < vKey.size(); i++)
        {        
            vPart.push_back(vKey[i]);
        }

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (vKey.size() != vBuffer.size())
        {
            iRet = -1;

            return iRet;
        }
        
        for (size_t i = 0; i < vKey.size(); i++)
        {
            if (vBuffer[i].first < 0)
            {
                vNoKey.push_back(vKey[i]);
            }
            else
            {
                mValues[vKey[i]] = vBuffer[i].second;
            }
        }
        
        return iRet;
    }

    /**
    * @brief incr
    *  
    * @param sKey        
    * @param iResult        
    * @return 0 成功 -1 失败
    */
    int incr(const string& sKey, int64_t& iResult)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("INCR");
        vPart.push_back(sKey);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iResult = TC_Common::strto<int64_t>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief incrby
    *  
    * @param sKey        
    * @param iInrement        
    * @param iResult        
    * @return 0 成功 -1 失败
    */
    int incrby(const string& sKey, int iInrement, int64_t& iResult)
    {
        int iRet = -1;

        string sCommand;

        vector<string> vPart;

        vPart.push_back("INCRBY");
        vPart.push_back(sKey);
        vPart.push_back(TC_Common::tostr(iInrement));

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iResult = TC_Common::strto<int64_t>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief decr
    *  
    * @param sKey        
    * @param iResult        
    * @return 0 成功 -1 失败
    */
    int decr(const string& sKey, int64_t& iResult)
    {
        int iRet = -1;

        string sCommand;

        vector<string> vPart;

        vPart.push_back("DECR");
        vPart.push_back(sKey);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iResult = TC_Common::strto<int64_t>(vBuffer[0].second);
        }

        return iRet;
    }

    
    /**
    * @brief decr
    *  
    * @param sKey        
    * @param sMember        
    * @param fValue        
    * @return 0 成功 -1 失败
    */
    int zAdd(const string& sKey, const string& sMember, const float& fValue)
    {
        int iRet = -1;

        string sCommand;

        vector<string> vPart;

        vPart.push_back("ZADD");
        vPart.push_back(sKey);
        vPart.push_back(TC_Common::tostr(fValue));
        vPart.push_back(sMember);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            int iSize = TC_Common::strto<int>(vBuffer[0].second);

            if (iSize == 0 || iSize == 1)
            {
                iRet = 0;
            }
            else
            {
                iRet = -1;
            }
        }

        return iRet;
    }

    /**
    * @brief zRem
    *  
    * @param sKey        
    * @param sMember        
    * @return 0 成功 -1 失败
    */
    int zRem(const string& sKey, const string& sMember)
    {
        int iRet = -1;

        string sCommand;

        vector<string> vPart;

        vPart.push_back("ZREM");
        vPart.push_back(sKey);
        vPart.push_back(sMember);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief zRem
    *  
    * @param sKey        
    * @param vMember        
    * @return 0 成功 -1 失败
    */
    int zRem(const string& sKey, const vector<string>& vMember)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("ZREM");
        vPart.push_back(sKey);

        for (size_t i = 0; i < vMember.size(); i++)
        {
            vPart.push_back(vMember[i]);
        }

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }
    
    /**
    * @brief zRangeByScore
    *  
    * @param sKey        
    * @param vKeyList        
    * @param fStart        
    * @param fEnd        
    * @return 0 成功 -1 失败
    */
    int zRangeByScore(const string& sKey, vector<pair<string, float> >& vKeyList, float fStart = FLT_MIN, float fEnd = FLT_MAX)
    {
        int iRet = -1;

        string sCommand;

        vector<string> vPart;

        vPart.push_back("ZRANGEBYSCORE");
        vPart.push_back(sKey);
        vPart.push_back(TC_Common::tostr(fStart));
        vPart.push_back(TC_Common::tostr(fEnd));
        vPart.push_back("WITHSCORES");

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (vBuffer.size() % 2 != 0)
        {
            return iRet;
        }
        
        for (size_t i = 0; i < vBuffer.size(); i += 2) 
        {
            vKeyList.push_back(make_pair(vBuffer[i].second, TC_Common::strto<float>(vBuffer[i+1].second)));
        }

        return iRet;
    }

    /**
    * @brief setNx
    *  
    * @param sKey        
    * @param sValue        
    * @return 0 成功 -1 失败
    */
    int setNx(const string& sKey, const string& sValue)
    {
        int iRet = -1;

        string sCommand;

        vector<string> vPart;

        vPart.push_back("SETNX");
        vPart.push_back(sKey);
        vPart.push_back(sValue);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief setExpire
    *  
    * @param sKey        
    * @param expir        
    * @return 0 成功 -1 失败
    */
    int setExpire(const string& sKey, unsigned int expir = 0)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("EXPIRE");
        vPart.push_back(sKey);
        vPart.push_back(TC_Common::tostr(expir));

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief getset
    *  
    * @param sKey        
    * @param sSetValue        
    * @param sReturnValue        
    * @return 0 成功 -1 失败
    */
    int getset(const string& sKey, const string& sSetValue, string& sReturnValue)
    {
        int iRet = -1;

        string sCommand;

        vector<string> vPart;

        vPart.push_back("GETSET");
        vPart.push_back(sKey);
        vPart.push_back(sSetValue);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = 0;

            if (vBuffer[0].first == -1)
            {
                iRet = 1;
            }
            else
            {
                sReturnValue = vBuffer[0].second;
                iRet = 0;
            }
        }
        else
        {
            iRet = -1;
        }

        return iRet;
    }

    /**
    * @brief list
    *  
    * @param sKey        
    * @param vValue        
    * @return 0 成功 -1 失败
    *  查找所有符合给定模式 pattern 的 key 
        KEYS * 匹配数据库中所有 key
        KEYS h?llo 匹配 hello ， hallo 和 hxllo 等
        KEYS h*llo 匹配 hllo 和 heeeeello 等。
        KEYS h[ae]llo 匹配 hello 和 hallo ，但不匹配 hillo 
    */
    int list(const string& sKey, vector<string>& vValue)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("KEYS");
        vPart.push_back(sKey);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0)
        {
            if (vBuffer.empty())
            {
                iRet = 1;
            }
            else
            {
                for (size_t i = 0; i < vBuffer.size(); ++i)
                {
                    if (vBuffer[i].first > 0)
                    {
                        vValue.push_back(vBuffer[i].second);
                    }
                }

                iRet = 0;
            }
        }

        return iRet;
    }

    /**
    * @brief hgetall
    *  
    * @param sKey        
    * @param mValue        
    * @return 0 成功 -1 失败
    * 返回哈希表 key中，所有的域和值。在返回值里，紧跟每个域名(field name)之后是域的值(value)，所以返回值的长度是哈希表大小的两倍。
    */
    int hgetall(const string& sKey, map<string, string>& mValue)
    {
        int iRet = -1;

        string sCommand;

        vector<string> vPart;

        vPart.push_back("HGETALL");
        vPart.push_back(sKey);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0)
        {
            if (vBuffer.empty())
            {
                iRet = 1;
            }
            else
            {
                iRet = 0;

                for (size_t i = 0; i < vBuffer.size(); i += 2)
                {
                    if (i + 1 < vBuffer.size())
                    {
                        mValue[vBuffer[i].second] = vBuffer[i+1].second;
                    }
                }
            }
        }

        return iRet;
    }

    /**
    * @brief hget
    *  
    * @param sKey        
    * @param mValue        
    * @return 0 成功 1-不存在 -1 失败
    */
    int hget(const string& sKey, const string& sField, string& sValue)
    {
        int iRet = -1;

        string sCommand;

        vector<string> vPart;

        vPart.push_back("HGET");
        vPart.push_back(sKey);
        vPart.push_back(sField);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            if (vBuffer[0].first == -1)
            {
                iRet = 1;
            }
            else
            {
                sValue = vBuffer[0].second;
                iRet = 0;
            }
        }
        else
        {
            iRet = 1;
        }

        return iRet;
    }

    /**
    * @brief hdel
    *  
    * @param sKey        
    * @param sField        
    * @return 0 成功 -1 失败
    */
    int hdel(const string& sKey, const string& sField)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("HDEL");
        vPart.push_back(sKey);
        vPart.push_back(sField);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    // 0-不存在 1-存在
    int hexists(const string& sKey, const string& sField)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("HEXISTS");
        vPart.push_back(sKey);
        vPart.push_back(sField);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief hmset
    *  
    * @param sKey        
    * @param mValue        
    * @return 0 成功 -1 失败
    * 同时将多个field-value(域-值)对设置到哈希表 key中。此命令会覆盖哈希表中已存在的域。如果 key不存在，一个空哈希表被创建并执行 HMSET操作。
    */
    int hmset(const string& sKey, const map<string, string>& mValue)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("HMSET");
        vPart.push_back(sKey);

        for (map<string, string>::const_iterator iter = mValue.begin(); iter != mValue.end(); ++iter)
        {
            vPart.push_back(iter->first);
            vPart.push_back(iter->second);
        }

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = 0;
        }
        else
        {
            iRet = -1;
        }

        return iRet;
    }

    /**
    * @brief hmset
    *  
    * @param sKey        
    * @param mValue        
    * @return 0 成功 -1 失败
    * 将哈希表 key中的域field的值设为value。如果 key不存在，一个新的哈希表被创建并进行 HSE 操作。 如果域 field已经存在于哈希表中，旧值将被覆盖。
    */
    int hset(const string& sKey, const string& sField, const string& sValue)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("HSET");
        vPart.push_back(sKey);
        vPart.push_back(sField);
        vPart.push_back(sValue);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief hincby
    *  
    * @param sKey        
    * @param sField
    * @param sAddValue        
    * @return 0 成功 -1 失败
    */
    int hincby(const string& sKey, const string& sField, const string& sAddValue)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("HINCRBY");
        vPart.push_back(sKey);
        vPart.push_back(sField);
        vPart.push_back(sAddValue);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief zscore
    *  
    * @param sKey        
    * @param sField
    * @param sValue        
    * @return 0 成功 -1 失败
    * 返回有序集 key中，成员 member的score值。如果member元素不是有序集key的成员，或key不存在，返回 nil。
    */
    int zscore(const string& sKey, const string sField, string& sValue)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("ZSCORE");
        vPart.push_back(sKey);
        vPart.push_back(sField);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            sValue = vBuffer[0].second;
        }

        return iRet;
    }

    /**
    * @brief sadd
    *  
    * @param sKey        
    * @param vField
    * @return 被添加到集合中的新元素的数量，不包括被忽略的元素
    * 将一个或多个member元素加入到集合key当中，已经存在于集合的member元素将被忽略。假如key不存在，则创建一个只包含member元素作成员的集合。当key不是集合类型时，返回一个错误。
    */
    int sadd(const string& sKey, const vector<string>& vField)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("SADD");
        vPart.push_back(sKey);

        for (size_t i = 0; i < vField.size(); ++i)
        {
            vPart.push_back(vField[i]);
        }

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief srem
    *  
    * @param sKey        
    * @param vField
    * @return 被成功移除的元素的数量，不包括被忽略的元素。
    * 移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略。
    */
	int srem(const string& sKey, const vector<string>& vField)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("SREM");
        vPart.push_back(sKey);

        for (size_t i = 0; i < vField.size(); ++i)
        {
            vPart.push_back(vField[i]);
        }

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief srem
    *  
    * @param sKey        
    * @param vField
    * @return 0 成功 -1 失败
    * 移除并返回集合中的一个随机元素。 如果只想获取一个随机元素，但不想该元素从集合中被移除的话，可以使用 SRANDMEMBER 命令。
    */
    int spop(const string& sKey, string& sValue)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("SPOP");
        vPart.push_back(sKey);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            sValue = vBuffer[0].second;
        }

        return iRet;
    }

    /**
    * @brief srem
    *  
    * @param sKey        
    * @param iMemberNum 返回集合中元素个数
    * @return 0 成功 -1 失败
    */
    int scard(const string& sKey, int& iMemberNum)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("SCARD");
        vPart.push_back(sKey);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iMemberNum = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief sdiff
    *  
    * @param sKey        
    * @param vKey
    * @param vValue
    * @return 0 成功 -1 失败
    */
    int sdiff(const string& sKey, const vector<string>& vKey, vector<string>& vValue)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("SDIFF");
        vPart.push_back(sKey);

        std::copy(vKey.begin(), vKey.end(), back_inserter(vPart));

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0)
        {
            for (size_t i = 0; i < vBuffer.size(); ++i)
            {
                vValue.push_back(vBuffer[i].second);
            }
        }

        return iRet;
    }

    /**
    * @brief smembers
    *  
    * @param sKey        
    * @param vValue
    * @return 返回成员个数
    * 返回集合key中的所有成员。不存在的key被视为空集合。
    */
    int smembers(const string& sKey, vector<string>& vValue)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("SMEMBERS");
        vPart.push_back(sKey);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0)
        {
            for (size_t i = 0; i < vBuffer.size(); ++i)
            {
                vValue.push_back(vBuffer[i].second);
            }

            iRet = vBuffer.size();
        }

        return iRet;
    }

    /**
    * @brief sismember
    *  
    * @param sKey        
    * @param sMember
    * @return 如果 member 元素是集合的成员，返回 1 。 如果 member 元素不是集合的成员，或 key 不存在，返回 0 。
    */
    int sismember(const string& sKey, const string& sMember)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("SISMEMBER");
        vPart.push_back(sKey);
        vPart.push_back(sMember);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief zrange
    *  
    * @param sKey        
    * @param iStart
    * @param iStop
    * @param bWithScores
    * @param vValue
    * @return 
    * 返回有序集 key中，指定区间内的成员。下标参数 start和 stop都以0为底，也就是说，以 0表示有序集第一个成员，以 1表示有序集第二个成员，以此类推。你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。
    * 可以通过使用 WITHSCORES 选项，来让成员和它的 score 值一并返回，返回列表以 value1,score1, ..., valueN,scoreN 的格式表示。
    */
    int zrange(const string& sKey, int iStart, int iStop, bool bWithScores, vector<pair<string, float> >& vValue)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("ZRANGE");
        vPart.push_back(sKey);
        vPart.push_back(TC_Common::tostr(iStart));
        vPart.push_back(TC_Common::tostr(iStop));

        if (bWithScores)
        {
            vPart.push_back("WITHSCORES");
        }

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0)
        {
            if (bWithScores)
            {
                for (size_t i = 0; i < vBuffer.size(); i += 2)
                {
                    vValue.push_back(make_pair(vBuffer[i].second, TC_Common::strto<float>(vBuffer[i+1].second)));
                }
            }
            else
            {
                for (size_t i = 0; i < vBuffer.size(); ++i)
                {
                    vValue.push_back(make_pair(vBuffer[i].second, 0));
                }
            }
        }

        return iRet;
    }

    /**
    * @brief lpush
    *  
    * @param sKey        
    * @param vValue
    * @return 0 成功 -1 失败
    */
    int lpush(const string& sKey, const vector<string>& vValue)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("LPUSH");
        vPart.push_back(sKey);

        for (size_t i = 0; i < vValue.size(); ++i)
        {
            vPart.push_back(vValue[i]);
        }

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief rpush
    *  
    * @param sKey        
    * @param vValue
    * @return 0 成功 -1 失败
    */
    int rpush(const string& sKey, const vector<string>& vValue)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("RPUSH");
        vPart.push_back(sKey);

        for (size_t i = 0; i < vValue.size(); ++i)
        {
            vPart.push_back(vValue[i]);
        }

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief ltrim
    *  
    * @param sKey        
    * @param iStart
    * @param iStop
    * @return 0 trim size
    *  对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
    */
    int ltrim(const string& sKey, int iStart, int iStop)
    {   
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("LTRIM");
        vPart.push_back(sKey);
        vPart.push_back(TC_Common::tostr(iStart));
        vPart.push_back(TC_Common::tostr(iStop));

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            iRet = TC_Common::strto<int>(vBuffer[0].second);
        }

        return iRet;
    }

    /**
    * @brief lpop
    *  
    * @param sKey        
    * @param sValue
    * @return 0 成功 -1 失败 
    */
    int lpop(const string& sKey, string& sValue)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("LPOP");
        vPart.push_back(sKey);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            sValue = vBuffer[0].second;
        }

        return iRet;
    }

    /**
    * @brief rpop
    *  
    * @param sKey        
    * @param sValue
    * @return 0 成功 -1 失败 
    */
    int rpop(const string& sKey, string& sValue)
    {
        int iRet = -1;
        string sCommand;

        vector<string> vPart;

        vPart.push_back("RPOP");
        vPart.push_back(sKey);

        buildCommand(vPart, sCommand);

        vector<pair<int, string> > vBuffer;

        iRet = doCommand(sCommand, vBuffer);

        if (iRet == 0 && vBuffer.size() == 1)
        {
            sValue = vBuffer[0].second;
        }

        return iRet;
    }
private:
    void buildCommand(const vector<string>& vPart, string& sCommand)
    {
        stringstream ss;
        ss << "*" << vPart.size() << "\r\n";

        for (size_t i = 0; i < vPart.size(); i++)
        {
            ss << "$" << vPart[i].size() << "\r\n" << vPart[i] << "\r\n";
        }

        sCommand = ss.str();
    }

    int doCommand(const string& sCommand, vector<pair<int, string> >& vBuffer)
    {
        int iRet = -1;

        shared_ptr<TC_CustomProtoReq> req = std::make_shared<RedisReq>();
        req->sendBuffer(sCommand);

        shared_ptr<TC_CustomProtoRsp> rsp = std::make_shared<RedisRsp>();
        common_protocol_call("redis", req, rsp);

        string sBuffer = rsp->getBuffer();
        string sSep = "\r\n";
        string sData;
        size_t iPos;
        
        if (sBuffer.empty())
        {
            return iRet;
        }

        char f = sBuffer[0];
        switch (f)
        {
        case '+':
            iPos = sBuffer.find(sSep);

            if (iPos != string::npos)
            {
                sData = sBuffer.substr(1, iPos);
                iRet = 0;
                vBuffer.push_back(make_pair(sData.size(), sData));

                return iRet;
            }

            iRet = -1;
            break;

        case '-':
            iPos = sBuffer.find(sSep);

            if (iPos != string::npos)
            {
                sData = sBuffer.substr(1, iPos);
                iRet = -1;
                vBuffer.push_back(make_pair(sData.size(), sData));

                LOG_CONSOLE_DEBUG << "iRet:" << iRet << " sData:" << sData << endl;

                return iRet;
            }

            iRet = -1;

            break;
        case ':':
            iPos = sBuffer.find(sSep);

            if (iPos != string::npos)
            {
                sData = sBuffer.substr(1, iPos);
                iRet = 0;
                vBuffer.push_back(make_pair(sData.size(), sData));

                return iRet;
            }

            iRet = -1;

            break;
        case '$':
            iPos = sBuffer.find(sSep);

            if (iPos != string::npos)
            {
                int iLen = TC_Common::strto<size_t>(sBuffer.substr(1, iPos));

                string sDataBuffer = sBuffer.substr(iPos + sSep.size());

                if (iLen > 0 && sDataBuffer.size() == (iLen + sSep.size()))
                {
                    sData = sDataBuffer.substr(0, sDataBuffer.size() - sSep.size());

                    vBuffer.push_back(make_pair(sData.size(), sData));

                    iRet = 0;

                    return iRet;
                }
                else if (iLen == 0)
                {
                    vBuffer.push_back(make_pair(0, ""));

                    iRet = 0;

                    return iRet;
                }
                else if(iLen == -1)
                {
                    vBuffer.push_back(make_pair(-1, ""));

                    iRet = 0;

                    return iRet;
                }
            } 

            iRet = -1;

            break;
        case '*':
            iPos = sBuffer.find(sSep);

            if (iPos != string::npos)
            {
                int iLen = TC_Common::strto<size_t>(sBuffer.substr(1, iPos));

                string sDataBuffer = sBuffer.substr(iPos + sSep.size());

                iRet = doMultiReplay(sDataBuffer, iLen, vBuffer);

                if (iRet == 0)
                {
                    return iRet;
                }
                else
                {
                    break;
                }
            }

            iRet = -1;

            break;
        default:
            iRet = -1;
            break;
        }

        return iRet;
    }
   
    int doCommand(const string& sCommand)
    {
        int iRet = -1;

        shared_ptr<TC_CustomProtoReq> req = std::make_shared<RedisReq>();
        req->sendBuffer(sCommand);
        shared_ptr<TC_CustomProtoRsp> rsp = std::make_shared<RedisRsp>();
        
        common_protocol_call("redis", req, rsp);

        string sBuffer = rsp->getBuffer();

        string sRightRet = "+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n";
        
        string sConflictRet = "+OK\r\n+QUEUED\r\n*-1\r\n";

        if (sRightRet == sBuffer)
        {
            iRet = 0;
            return iRet;
        }
        else if (sConflictRet == sBuffer)
        {
            iRet = -1;
            return iRet;
        }
            
        return iRet;
    }

    int doMultiReplay(const string& sBuffer, const size_t& iSize, vector<pair<int, string> >& vBuffer)
    {
        int iRet = -1;
        string sSep = "\r\n";

        string sBufferStream = sBuffer;

        while (!sBufferStream.empty())
        {
            size_t iPos = sBufferStream.find(sSep);

            if (iPos != string::npos)
            {
                int iLen = TC_Common::strto<int>(sBufferStream.substr(1, iPos));

                if (iLen < 0)
                {
                    vBuffer.push_back(make_pair(iLen, ""));

                    sBufferStream = sBufferStream.substr(iPos + sSep.size());
                }
                else
                {
                    size_t iSubLen = iPos + sSep.size() + iLen + sSep.size();

                    if (iSubLen > sBufferStream.size())
                    {
                        break;
                    }
                    
                    string sDataBuffer = sBufferStream.substr(iPos + sSep.size(), iLen);

                    vBuffer.push_back(make_pair(sDataBuffer.size(), sDataBuffer));

                    sBufferStream = sBufferStream.substr(iSubLen);
                }
            }
            else
            {
                break;
            }
        }
        
        if (vBuffer.size() == iSize)
        {
            iRet = 0;
        }
        else
        {
            vBuffer.clear();
        }
        
        return iRet;
    }

    /**
    * 配置
    */
    TC_RDConf   _rdConf;
};
typedef tars::TC_AutoPtr<RedisProxy> RedisPrx;

}
#endif
