CXX := g++
CXXFLAGS := -O3 -std=c++17 -Iparlaylib/include -g

SRC := test_hash_nghs.cpp nghs_ht.h
TARGET := test

.PHONY: all clean rebuild

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) $< -o $@

rebuild: clean all

clean:
	rm -f $(TARGET)
