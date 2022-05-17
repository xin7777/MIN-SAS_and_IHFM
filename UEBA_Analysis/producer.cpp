#include <ndn-cxx/face.hpp>
#include <iostream>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <strings.h>
#include <string.h>
#include <arpa/inet.h>
#include "rdkafkacpp.h"
using namespace ndn ;
using namespace std ;

#define PORTNUM 5052
#define BUFSIZE 4096
#define oops(msg) {perror(msg);}

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb (RdKafka::Message &message) {
        /* If message.err() is non-zero the message delivery failed permanently
         * for the message. */
        if (message.err())
            std::cerr << "% Message delivery failed: " << message.errstr() << std::endl;
        else
            std::cerr << "% Message delivered to topic " << message.topic_name() <<
                      " [" << message.partition() << "] at offset " <<
                      message.offset() << std::endl;
    }
};

struct log_str{
    char string[BUFSIZE] = {0};
    int len;
};

void* send_packet(void* arg){
//    struct sockaddr_in saddr;
//    int sock_id, sock_fd;
    int messlen;

    /**
     * get a socket
     */
//    sock_id = socket(AF_INET, SOCK_STREAM, 0);
//    if (sock_id == -1)
//    oops("socket");
//
//    saddr.sin_port = htons(PORTNUM);
//    saddr.sin_family = AF_INET;
//    inet_aton("127.0.0.1", &saddr.sin_addr);
//
//    if (connect(sock_id, (struct sockaddr*)&saddr, sizeof(saddr)) != 0)
//    oops("connect");

    // messlen = read(sock_id, message, BUFSIZE);
    struct log_str arg2;
    arg2 = *((struct log_str*)arg);
    char * message = arg2.string;
    messlen = arg2.len;
    std::string brokers = "127.0.0.1:9092";
    std::string topic = "ueba_log";

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    std::string errstr;
    if (conf->set("bootstrap.servers", brokers, errstr) !=
        RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        exit(1);
    }

    ExampleDeliveryReportCb ex_dr_cb;

    if (conf->set("dr_cb", &ex_dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        exit(1);
    }

    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        exit(1);
    }

    delete conf;

    RdKafka::ErrorCode err =
            producer->produce(
                    /* Topic name */
                    topic,
                    /* Any Partition: the builtin partitioner will be
                     * used to assign the message to a topic based
                     * on the message key, or random partition if
                     * the key is not set. */
                    RdKafka::Topic::PARTITION_UA,
                    /* Make a copy of the value */
                    RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                    /* Value */
                    message, messlen,
                    /* Key */
                    NULL, 0,
                    /* Timestamp (defaults to current time) */
                    0,
                    /* Message headers, if any */
                    NULL,
                    /* Per-message opaque value passed to
                     * delivery report */
                    NULL);

    if (err != RdKafka::ERR_NO_ERROR) {
        std::cerr << "% Failed to produce to topic " << topic << ": " <<
                  RdKafka::err2str(err) << std::endl;

        if (err == RdKafka::ERR__QUEUE_FULL) {
            /* If the internal queue is full, wait for
             * messages to be delivered and then retry.
             * The internal queue represents both
             * messages to be sent and messages that have
             * been sent or failed, awaiting their
             * delivery report callback to be called.
             *
             * The internal queue is limited by the
             * configuration property
             * queue.buffering.max.messages */
            producer->poll(1000/*block for max 1000ms*/);
        }

    } else {
        std::cerr << "% Enqueued message (" << messlen << " bytes) " <<
                  "for topic " << topic << std::endl;
    }
    producer->poll(0);
    std::cerr << "% Flushing final messages..." << std::endl;
    producer->flush(10*1000 /* wait for max 10 seconds */);

    if (producer->outq_len() > 0)
        std::cerr << "% " << producer->outq_len() <<
                  " message(s) were not delivered" << std::endl;

    delete producer;
//    if (messlen == -1)
//        oops("read");
//    if (write(sock_id, message, messlen) != messlen)
//        oops("write");
//    close(sock_id);
    return NULL;
}

class Producer : noncopyable
{
	public:
		void run()
		{
			std::cout << "pro start " << std::endl ;
			m_face.setInterestFilter("/min/B/SAS",
					bind(&Producer::onInterest, this, _1, _2),
					RegisterPrefixSuccessCallback(),
					bind(&Producer::onRegisterFailed, this, _1, _2));
			m_face.processEvents();
		}

	private:
		void onInterest(const InterestFilter& filter, const Interest& interest)
		{
			std::cout << "<< I: " << interest << std::endl;
			printf("%dth packet received", ++this->count);

			//if(interest.hasParameters()){   // 0.6.5版本ndn-cxx
				//Block b(interest.getParameters().value() , 
						//interest.getParameters().value_size()) ;
				//cout << b.type() << endl ;
				//cout << b.value_size() << endl ;
			//}
			if(interest.hasApplicationParameters()){	 // 0.6.6版本ndn-cxx
                struct log_str* arg;
                arg = (struct log_str*)malloc(sizeof(struct log_str));
                arg->len = interest.getApplicationParameters().value_size();
                // strcpy(arg.string, (char *)interest.getApplicationParameters().value());
                strcpy(arg->string, (char *)interest.getApplicationParameters().value());
                printf("%d %s\n", arg->len, arg->string);
                pthread_t t1;
                pthread_create(&t1, NULL, send_packet, arg);
			}

			Name dataName(interest.getName());
			dataName
				.append("testApp") // add "testApp" component to Interest name
				.appendVersion();  // add "version" component (current UNIX timestamp in milliseconds)

			static const std::string content = "RECEIVED";

			shared_ptr<Data> data = make_shared<Data>();
			data->setName(dataName);
			data->setFreshnessPeriod(0_s); // 10 seconds
			// 数据缓存在节点中，立即变旧

			data->setContent(reinterpret_cast<const uint8_t*>(content.data()), content.size());


			m_keyChain.sign(*data);
			std::cout << ">> D: " << *data << std::endl;
			m_face.put(*data);
		}


		void onRegisterFailed(const Name& prefix, const std::string& reason)
		{
			std::cerr << "ERROR: Failed to register prefix \""
				<< prefix << "\" in local hub's daemon (" << reason << ")"
				<< std::endl;
			m_face.shutdown();
		}

	private:
		Face m_face;
		KeyChain m_keyChain;
		int count = 0;
};



int main(int argc, char** argv)
{
	Producer producer;
	try {
		producer.run();
	}
	catch (const std::exception& e) {
		std::cerr << "ERROR: " << e.what() << std::endl;
	}
	return 0;
}
