#include <ndn-cxx/face.hpp>
#include <iostream>
#include <python3.6m/Python.h>
#include <pthread.h>
using namespace ndn ;
using namespace std ;

struct module_state {
    PyObject *error;
};
#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))
class Producer : noncopyable
{
	public:
		void run()
		{
			std::cout << "pro start " << std::endl ;
			m_face.setInterestFilter("/MIN-VPN/testflow/SAS",
					bind(&Producer::onInterest, this, _1, _2),
					RegisterPrefixSuccessCallback(),
					bind(&Producer::onRegisterFailed, this, _1, _2));
			m_face.processEvents();
		}
		PyObject* getLogs()
		{
			return this->buf;
		}

	private:
		void onInterest(const InterestFilter& filter, const Interest& interest)
		{
			std::cout << "<< I: " << interest << std::endl;

			//if(interest.hasParameters()){   // 0.6.5版本ndn-cxx
			//Block b(interest.getParameters().value() , 
			//interest.getParameters().value_size()) ;
			//cout << b.type() << endl ;
			//cout << b.value_size() << endl ;
			//}
			if(interest.hasApplicationParameters()){	 // 0.6.6版本ndn-cxx
				string intParaStr((char *)
						interest.getApplicationParameters().value(),
						interest.getApplicationParameters().value_size());
				std::cout << ">> paras:" << intParaStr << std::endl;
				PyList_Append(buf,
					PyUnicode_FromStringAndSize(
						(char *)interest.getApplicationParameters().value(),
						interest.getApplicationParameters().value_size()
					)
				);
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
			// std::cout << ">> D: " << *data << std::endl;
			m_face.put(*data);
//			counter += 1;
//			if(counter == 2)
//			{
//				m_face.shutdown();
//			}
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
		PyObject* buf = PyList_New(0);
		// int counter = 0;
};


Producer producer;
void* run(void* ptr){
    producer.run();
}
PyObject* collect(PyObject* self, PyObject* args)
{
    try
    {
//        int kill_rc = pthread_kill(t1, 0);

//        if(pthread_tryjoin_np(t1, NULL) == 0)
        pthread_t t1;
        if (t1 == -1)
            pthread_create(&t1, NULL, run, NULL);
        sleep(10);
        pthread_kill(t1, SIGKILL);
        printf("killed");
        sleep(10);
        return producer.getLogs();
    }
    catch (const std::exception& e)
    {
        std::cerr << "ERROR: " << e.what() << std::endl;
    }
    // return producer.getLogs();
}


static PyMethodDef producer_methods[] = {
 {"Collect", collect, METH_VARARGS, "something"},
 {NULL, NULL}
};



extern "C"{
    #if PY_MAJOR_VERSION >= 3
    static int producer_traverse(PyObject *m, visitproc visit, void *arg) {
        Py_VISIT(GETSTATE(m)->error);
        return 0;
    }

    static int producer_clear(PyObject *m) {
        Py_CLEAR(GETSTATE(m)->error);
        return 0;
    }


    static struct PyModuleDef moduledef = {
            PyModuleDef_HEAD_INIT,
            "producer",
            NULL,
            sizeof(struct module_state),
            producer_methods,
            NULL,
            producer_traverse,
            producer_clear,
            NULL
    };
    #define INITERROR return NULL

    PyObject *
    PyInit_producer(void)

    #else
    #define INITERROR return

    void
    initproducer(void)
    #endif
    {
    #if PY_MAJOR_VERSION >= 3
        PyObject *module = PyModule_Create(&moduledef);
    #else
        PyObject *module = Py_InitModule("producer", myextension_methods);
    #endif

        if (module == NULL)
            INITERROR;
        struct module_state *st = GETSTATE(module);

        st->error = PyErr_NewException("producer.Error", NULL, NULL);
        if (st->error == NULL) {
            Py_DECREF(module);
            INITERROR;
        }

    #if PY_MAJOR_VERSION >= 3
        return module;
    #endif
    }
}

