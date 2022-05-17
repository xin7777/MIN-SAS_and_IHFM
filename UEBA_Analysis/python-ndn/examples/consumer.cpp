#include <ndn-cxx/face.hpp>
#include <iostream>

// Enclosing code in ndn simplifies coding (can also use `using namespace ndn`)
	// Additional nested namespaces can be used to prevent/limit name conflicts

using namespace::ndn ;
using namespace::std ;

class Consumer : noncopyable
{
	public:
		void run()
		{
			Interest interest(Name("/localmanager/mircertification"));
			interest.setInterestLifetime(1_s); // 2 seconds
			interest.setMustBeFresh(true);
			
			string param = "{'Code':5, 'Data':{'KeyLocator':'/testApp/%FD%00%00%01t%F6%B0%C5%81'}}" ;
			
			interest.setApplicationParameters((const uint8_t*)param.data(), 
					param.size()) ;
			
			m_keyChain.sign(interest) ;

			m_face.expressInterest(interest,
					bind(&Consumer::onData, this,  _1, _2),
					bind(&Consumer::onNack, this, _1, _2),
					bind(&Consumer::onTimeout, this, _1));

			std::cout << "Sending " << interest.getName().toUri() << std::endl;

			// processEvents will block until the requested data received or timeout occurs
			m_face.processEvents();
		}

	private:
		void onData(const Interest& interest, const Data& data)
		{
			std::cout << data.getName().toUri() << std::endl;
			string content((char*)data.getContent().value() , 
					data.getContent().size()-1);
			//cout << "getSignature = " << data.getSignature().getInfo() << endl ;
			cout << "content size = " << data.getContent().size() << endl ;
			cout << content << endl ;
			//std::cout << data.getContent().value() << std::endl ;
		}

		void onNack(const Interest& interest, const lp::Nack& nack)
		{
			std::cout << "received Nack with reason " << nack.getReason()
				<< " for interest " << interest << std::endl;
		}

		void onTimeout(const Interest& interest)
		{
			std::cout << "Timeout " << interest << std::endl;
		}

	private:
		Face m_face;
		KeyChain m_keyChain ;
};


int main(int argc, char** argv)
{
	Consumer consumer;
	try {
		consumer.run();
	}
	catch (const std::exception& e) {
		std::cerr << "ERROR: " << e.what() << std::endl;
	}
	return 0;
}
