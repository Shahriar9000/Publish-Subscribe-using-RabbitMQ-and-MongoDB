#include <iostream>
#include <ev.h>
#include <amqpcpp.h>
#include <amqpcpp/linux_tcp.h>
#include <amqpcpp/libev.h>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <fstream>
#include <string>
#include <jsoncpp/json/json.h>
#include <vector>
using namespace std;

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


int main()
{
	
	mongocxx::instance inst{};   // This should be done only once.

	// access to the event loop
	auto *loop = EV_DEFAULT;

	// handler for libev (so we don't have to implement AMQP::TcpHandler!)
	MyHandler handler(loop);

	// make a connection
	AMQP::TcpConnection connection(&handler, AMQP::Address("amqp://guest:guest@localhost/"));

	// THIS IS A CHANNEL OBJECT
	AMQP::TcpChannel channel(&connection);

	

	// callback function that is called when the consume operation starts
	auto startCb = [](const string &consumertag) {

		cout << "consume operation started" << endl;


	};

	// callback function that is called when the consume operation failed
	auto errorCb = [](const char *message) {

	   cout << "consume operation failed" << endl;
	};

	// callback operation when a message was received
	auto messageCb = [&channel](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) 
	{

		cout << "message received " << message.routingkey();

		string routing_key = message.routingkey();

		// creating string from message body
		const char* body = message.body();
		int body_size = message.bodySize();
		string str_json = "";
		for (int i = 0; i < body_size; i++)
		{
			 //cout << *(body+i);
			 str_json += *(body+i);
		} 
		cout << " " << str_json << endl;

		// parse json string to c++ readable object
		Json::Value root;   
		Json::Reader reader;
		bool parsingSuccessful = reader.parse( str_json.c_str(), root );     //parse process
		if ( !parsingSuccessful )
		{
			cout  << "Failed to parse" << reader.getFormattedErrorMessages();
		}

		// connecting to mongo server
		// The default mongocxx::uri constructor will connect to a server running on localhost on port 27017
		mongocxx::uri uri("mongodb://localhost:27017");
		mongocxx::client client(uri);

		// inserting to appropriate collection according to routing key
		// all other messages except for mtl.grade, mtl.temperature, mtl.health is not stored but discarded
		if (routing_key == "mtl.temperature")
		{
			string coll = "temperature";
			auto builder = bsoncxx::builder::stream::document{};
   
			bsoncxx::document::value doc_value = builder
			  << "timestamp" << root["timestamp"].asString()
			  << "temperature" << root["temperature"].asString()
			  << bsoncxx::builder::stream::finalize;
		
			auto collection = client["mtldb"][coll];
			collection.insert_one(doc_value.view());
		}
		else if (routing_key == "mtl.health")
		{
			string coll = "health";
			auto builder = bsoncxx::builder::stream::document{};
   
			bsoncxx::document::value doc_value = builder
			  << "timestamp" << root["timestamp"].asString()
			  << "health_status" << root["health_status"].asString()
			  << bsoncxx::builder::stream::finalize;
		
			auto collection = client["mtldb"][coll];
			collection.insert_one(doc_value.view());
		}
		else if (routing_key == "mtl.grade")
		{
			string coll = "grade";
			Json::Value grades = root["grades"];

			auto builder = bsoncxx::builder::stream::document{};
   
			bsoncxx::document::value doc_value = builder
			  << "timestamp" << root["timestamp"].asString()
			  << "grades" 
				  << bsoncxx::builder::stream::open_array
					  << bsoncxx::builder::stream::open_document 
						  << "ni" << grades[0]["ni"].asString()
						  << "cu" << grades[0]["cu"].asString()
					  << bsoncxx::builder::stream::close_document
				  << bsoncxx::builder::stream::close_array
			  << bsoncxx::builder::stream::finalize;
		
			auto collection = client["mtldb"][coll];
			collection.insert_one(doc_value.view());
		}
		else // routing key: mtl
		{
			// do nothing
		}

		// acknowledge the message to remove from queue
		channel.ack(deliveryTag);
		
	};

	// start consuming from the queue, and install the callbacks
	channel.consume("queue")
		.onReceived(messageCb)
		.onSuccess(startCb)
		.onError(errorCb);

	

	// run the loop for further consumption if something is published later
	ev_run(loop, 0);
	connection.close();
	return 0;
}