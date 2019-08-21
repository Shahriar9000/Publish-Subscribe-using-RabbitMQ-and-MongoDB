#include <iostream>
#include <pthread.h>
#include <stdlib.h>
#include <vector>

using namespace std;



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
	
	pthread_t threads[num_thread];

	for (int i = 0; i < num_thread; i++)
	{
		
		int ret = pthread_create(&threads[i], &attr, Publish, &filename[i]);
	

		if(ret != 0) 
    	{
        	cout << "Error creating thread " << i << endl;
        	
    	}
	}

	for (int i = 0; i < num_thread; i++)
	{
		pthread_join(threads[i], NULL);
	}












	return 0;
}