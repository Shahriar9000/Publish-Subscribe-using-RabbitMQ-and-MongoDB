#include <iostream>
#include <fstream>
#include <string>
#include <jsoncpp/json/json.h>
#include <vector>
#include <ev.h>
#include <amqpcpp.h>
#include <amqpcpp/linux_tcp.h>
#include <amqpcpp/libev.h>
#include <thread>
#include <mutex>
#include <stdlib.h>
using namespace std;

mutex m_lock;  
// tcp handler
class MyHandler : public AMQP::LibEvHandler
{
private:
	/**
	 *  Method that is called when a connection error occurs
	 *  @param  connection
	 *  @param  message
	 */
	virtual void onError(AMQP::TcpConnection *connection, const char *message) override
	{
		cout << "error: " << message << endl;
	}

	/**
	 *  Method that is called when the TCP connection ends up in a connected state
	 *  @param  connection  The TCP connection
	 */
	virtual void onConnected(AMQP::TcpConnection *connection) override 
	{
		cout << "connected" << endl;
	}

	/**
	 *  Method that is called when the TCP connection ends up in a ready
	 *  @param  connection  The TCP connection
	 */
	virtual void onReady(AMQP::TcpConnection *connection) override 
	{
		cout << "ready" << endl;
	}

	/**
	 *  Method that is called when the TCP connection is closed
	 *  @param  connection  The TCP connection
	 */
	virtual void onClosed(AMQP::TcpConnection *connection) override 
	{
	   cout << "closed" << endl;
	}

	/**
	 *  Method that is called when the TCP connection is detached
	 *  @param  connection  The TCP connection
	 */
	virtual void onDetached(AMQP::TcpConnection *connection) override 
	{
		cout << "detached" << endl;
	}
	
	
public:
	/**
	 *  Constructor
	 *  @param  ev_loop
	 */
	MyHandler(struct ev_loop *loop) : AMQP::LibEvHandler(loop) {}

	/**
	 *  Destructor
	 */
	virtual ~MyHandler() = default;
};

void* Publish (void * json_file)
{
	string filename ="";
	filename = *((string*)json_file);

	// select routing key depending on filename
	string routing_key;
	if (filename == "mtl_temperature.json")
		routing_key = "mtl.temperature";
	else if (filename == "mtl_health.json")
		routing_key = "mtl.health";
	else if (filename == "mtl_grade.json")
		routing_key = "mtl.grade";
	else
		routing_key = "mtl";

	// access to the event loop

	m_lock.lock();
	auto *loop = EV_DEFAULT;
	
	// handler implemented for handling tcpconnection
	MyHandler handler(loop);

	// make a connection
	AMQP::TcpConnection connection(&handler, AMQP::Address("amqp://guest:guest@localhost/"));

	//THIS IS A CHANNEL OBJECT
	AMQP::TcpChannel channel(&connection);

	cout << "connection  and channel is open" << endl;

	// declare queue
	channel.declareQueue("queue").onSuccess([&connection](const std::string &name, uint32_t messagecount, uint32_t consumercount){

		// report the name of the temporary queue
		cout << "declared queue " << endl;

		connection.close();

	});

	// declare exchange with topic routing
	channel.declareExchange("exchange", AMQP::topic).onSuccess([](){

		// report the name of the temporary queue
	   cout << "declared first message " << endl;
		
	});
	
	//Binding queue with exchange
	channel.bindQueue("exchange", "queue", "mtl.#");
   
   	// reading and parse file
	ifstream ifs(filename);
	Json::Reader reader;
	Json::Value obj;		// will contains the first obj of the array
	reader.parse(ifs, obj);     // Reader can also read strings

	// this is for creating string from a json object
	Json::StreamWriterBuilder wbuilder;
	wbuilder["indentation"] = "";

	for (int i = 0; i < obj.size(); ++i)
	{
		// each json obj will be converted to string
		string document = Json::writeString(wbuilder, obj[i]);


		// put transaction for every publish so that if multiple threads run this program to publish from multiple file,
		// all the threads could concurrently publish from different files rather than the thread follow a serial execution
		channel.startTransaction();

		channel.publish("exchange", routing_key, document);

		channel.commitTransaction()
		.onSuccess([]() 
		{
			cout << "all messages were successfully published" << endl;
		})
		.onError([](const char *message) 
		{
			cout << *message << endl;
		});
	

	}


	// wwait for connection to close
	ev_run(loop, 0);
	
	
	m_lock.unlock();
	

	pthread_exit(NULL);
}


int main(int argc, char const *argv[])
{
	if (argc < 3)
	{
		cout << "invalid argument\n";
		return 1;
	}


	pthread_attr_t attr;
	pthread_attr_init(&attr);
	int num_thread = atoi(argv[1]);
	vector<string> filename;


	for (int i = 0; i < num_thread; ++i)
	{
		filename.push_back(argv[i+2]);
	}
	
	pthread_t publisher[num_thread];

	for (int i = 0; i < num_thread; i++)
	{
		
		int ret = pthread_create(&publisher[i], &attr, Publish, &filename[i]);
		

		if(ret != 0) 
    	{
        	cout << "Error creating thread " << i << endl;
        	
    	}
	}

	for (int i = 0; i < num_thread; i++)
	{
		pthread_join(publisher[i], NULL);
	}


	return 0;
}