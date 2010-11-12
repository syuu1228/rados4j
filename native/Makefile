JAVA_HOME = /usr/lib/jvm/java-6-openjdk

INCLUDES_JAVA = \
-I$(JAVA_HOME)/include \
-I$(JAVA_HOME)/include/linux

INCLUDES_LOCAL = -I.

INCLUDES = $(INCLUDES_JAVA) $(INCLUDES_LOCAL)

CXXFLAGS = -Wall -g $(INCLUDES) -D_FILE_OFFSET_BITS=64 -O0 -fPIC
LDFLAGS = -Wall -g

JNI_LDFLAGS = -Wall -g -shared

TARGET = ../target/librados4j.so

SRC = com_dokukino_rados4j_Rados.cpp com_dokukino_rados4j_Pool.cpp com_dokukino_rados4j_ListCtx.cpp

OBJ = $(SRC:.cpp=.o)

LIBS = -lcrypto -lrados -lcrush

all: $(TARGET)

$(TARGET) : $(OBJ)
	$(CXX) $(JNI_LDFLAGS) -o $@ $(OBJ) $(LIBS)

clean:
	rm -f $(OBJ) $(TARGET)

depend:
	makedepend -Y -- $(INCLUDES_LOCAL) -- $(SRC) > /dev/null 2>&1

strip:
	strip $(TARGET)

test:


# DO NOT DELETE THIS LINE -- make depend depends on it.

com_dokukino_rados4j_Rados.o: com_dokukino_rados4j_Rados.h
com_dokukino_rados4j_Pool.o: com_dokukino_rados4j_Pool.h
com_dokukino_rados4j_ListCtx.o: com_dokukino_rados4j_ListCtx.h
