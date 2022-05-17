main: producer.cpp
	g++ -o transfer transfer.cpp -std=c++14 -lndn-cxx -lboost_system -lpthread
con: consumer.cpp
	g++ -o consumer consumer.cpp -std=c++14 -lndn-cxx -lboost_system
pro2: producer2.cpp
	g++ -o producer2 producer2.cpp -std=c++14 -lndn-cxx -lboost_system
pro: producer.cpp
	g++ -o producer producer.cpp -std=c++14 -lndn-cxx -lrdkafka -lrdkafka++ -lboost_system -lpthread
recv: recv.cpp
	g++ -o recv recv.cpp -std=c++14 -lndn-cxx -lboost_system -lpthread
send: send.cpp
	g++ -o send send.cpp -std=c++14 -lndn-cxx -lboost_system -lpthread

clean:
	rm send recv
