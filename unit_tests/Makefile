CXX = g++
CPPFLAGS += -I/usr/local/include -pthread -I../src
CXXFLAGS += -std=c++11 -g
LDFLAGS += -L/usr/local/lib `pkg-config --libs grpc++ grpc`       \
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed \
           -lprotobuf -lpthread -ldl

MAPREDUCE_LIB = -L../external/lib -lmapreduce 
MR_WORKER_LIB = -L../external/lib -lmr_worker

all: tests

mrdemo: main.o
	$(CXX) $^ -o $@ $(LDFLAGS) $(MAPREDUCE_LIB) $(LDFLAGS)

mr_worker: user_tasks.o
	$(CXX) $^ -o $@ $(LDFLAGS) $(MR_WORKER_LIB) $(LDFLAGS)

tests: tests.o
	$(CXX) $^ -o $@ $(LDFLAGS) $(MAPREDUCE_LIB) $(LDFLAGS)

%.o: %.cc
	$(CXX) -c $< -I../external/include $(CXXFLAGS)

clean:
	rm -f *.o *.pb.cc *.pb.h mrdemo mr_worker tests
