#include <iostream>
#include <string>  
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>
#include <vector>
#include <time.h>
using namespace std;
using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::open_document;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;
mongocxx::instance inst{};   // This should be done only once.



void get_health_temp(string x, string y)
{

    mongocxx::uri uri("mongodb://localhost:27017"); //The default mongocxx::uri constructor will connect to a server running on localhost on port 27017
    mongocxx::client client(uri);

    auto temperature_coll = client["mtldb"]["temperature"];
    auto health_coll = client["mtldb"]["health"];

    mongocxx::pipeline pipe{};
    
    
    pipe.lookup(
            make_document(
                    kvp("from", health_coll.name()),        
                    kvp("localField", "timestamp" ),
                    kvp("foreignField", "timestamp" ), 
                    kvp("as", "health_docs")
                ));
    pipe.match(
            make_document(
            kvp("timestamp", make_document(kvp("$gte", x))), 
            kvp("timestamp", make_document(kvp("$lte", y)))
            )
        );
    pipe.unwind("$health_docs");
    pipe.project(make_document(
         kvp("temperature", 1), 
         kvp("_id", 0),
         kvp("timestamp", 1),
        
         kvp("health_status", "$health_docs.health_status")));

    auto cursor = temperature_coll.aggregate(pipe, mongocxx::options::aggregate{});


 
    
    for(auto doc : cursor) 
    {
        std::cout << bsoncxx::to_json(doc) << "\n";
    }

}

void delete_health_temp(string x, string y)
{
    mongocxx::uri uri("mongodb://localhost:27017"); //The default mongocxx::uri constructor will connect to a server running on localhost on port 27017
    mongocxx::client client(uri);

    auto temperature_coll = client["mtldb"]["temperature"];
    auto health_coll = client["mtldb"]["health"];


    bsoncxx::stdx::optional<mongocxx::result::delete_result> result =
    health_coll.delete_many(
    document{} << "timestamp" << open_document <<
    "$gte" << x << "$lte" << y << close_document << finalize);

    if(result) {
    std::cout << result->deleted_count() << " objects deleted from health collection\n";
    }

    result = temperature_coll.delete_many(
    document{} << "timestamp" << open_document <<
    "$gte" << x << "$lte" << y << close_document << finalize);

    if(result) {
    std::cout << result->deleted_count() << " objects deleted from temperature collection\n";
    }



}
void insert_health_status(int health_status)
{

    mongocxx::uri uri("mongodb://localhost:27017"); //The default mongocxx::uri constructor will connect to a server running on localhost on port 27017
    mongocxx::client client(uri);

    
    time_t current_time = time(nullptr);
    asctime(localtime(&current_time));
    
  
    auto health_coll = client["mtldb"]["health"];
        
            
    bsoncxx::stdx::optional<mongocxx::result::insert_one> result = health_coll.insert_one (
        document{} << "timestamp" << current_time << "health_status" << health_status << finalize
    );
    
    if (result)
    {
        cout << "health_status " << health_status <<  " inserted at timestamp " << current_time <<endl;
    } 


}

void get_temp_with_health_status_not_0()
{

    mongocxx::uri uri("mongodb://localhost:27017"); //The default mongocxx::uri constructor will connect to a server running on localhost on port 27017
    mongocxx::client client(uri);

    auto temperature_coll = client["mtldb"]["temperature"];
    auto health_coll = client["mtldb"]["health"];

    mongocxx::pipeline pipe{};
    
    
    pipe.lookup(
            make_document(
                    kvp("from", health_coll.name()),        
                    kvp("localField", "timestamp" ),
                    kvp("foreignField", "timestamp" ), 
                    kvp("as", "health_docs")
                ));
    pipe.unwind("$health_docs");
    pipe.match(
            make_document(
            kvp("health_docs.health_status", make_document(kvp("$ne", "0")))
            )
        );
    
    pipe.project(make_document(
         kvp("temperature", 1), 
         kvp("_id", 0),
         kvp("timestamp", 1),
         kvp("health_status", "$health_docs.health_status"))
    );

    auto cursor = temperature_coll.aggregate(pipe, mongocxx::options::aggregate{});


 
    
    for(auto doc : cursor) 
    {
        std::cout << bsoncxx::to_json(doc) << "\n";
    }

}


int main() {
    
    
	cout  << "choose query from the following options: " << endl;
	cout  <<"[1] Get Temperature, Data and Health status between the UNIX timestamps X and Y" << endl;
	cout << "[2] Delete Temperature, Data and Health status between the UNIX timestamps X and Y" <<endl; 
	cout << "[3] Put a new health status at the current time" <<endl; 
	cout << "[4] Get all Temperature Data where Health Status is non-zero" << endl;

	int option = 0;
	auto a = 0, b = 0;

    do{
		cout << "option: ";
		cin >> option; 
		cout << endl;

		if (option == 1)
		{
			cout << "Value of timestamp X = ";
			cin >> a ; 
			cout << endl;
			cout << "Value of timestamp Y = ";
			cin >> b ; 
			cout << endl;
			string x = to_string(a);
    		string y = to_string (b);
			get_health_temp(x, y);
		}
		else if (option == 2)
		{
			cout << "Value of timestamp X = ";
			cin >> a; 
			cout << endl;
			cout << "Value of timestamp Y = ";
			cin >> b ; 
			cout << endl;
			string x = to_string(a);
    		string y = to_string (b);
			delete_health_temp(x, y);
		}
		else if (option == 3)
		{
			cout << "Insert Health Status: ";
			cin >> a ; 
			cout << endl;
			insert_health_status(a);
		}
		else if (option == 4)
		{
			get_temp_with_health_status_not_0();
		}
		else
		{
			cout << "wrong option, choose only from number 1-4" << endl;
		}
	}while(option < 1 || option > 4 );

    
  

    return 0;

}