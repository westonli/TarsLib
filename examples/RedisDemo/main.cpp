#include "servant/Application.h"
#include "TestRedisThread.h"
#include <iostream>

using namespace std;
using namespace tars;

int main(int argc,char**argv)
{
    try
    {
        int second = 10;
        
        if(argc > 1)
            second = TC_Common::strto<int>(argv[1]);
        
        if(second <=0 )
            second = 1;

		RedisThread thread(second);
		thread.start();

	    // RedisThread thread1(second);
		// thread1.start();

		thread.getThreadControl().join();
		// thread1.getThreadControl().join();
    }
    catch(std::exception&e)
    {
        cerr<<"std::exception:"<<e.what()<<endl;
    }
    catch(...)
    {
        cerr<<"unknown exception"<<endl;
    }
    return 0;
}
