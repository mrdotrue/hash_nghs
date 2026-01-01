CXX := g++

CXXFLAGS := -O3 -std=c++17 -Iparlaylib/include -g 

TEST_SRC := test_hash_nghs.cpp
UNIT_TEST := unit_test.cpp
BENCH_SRC := benchmark_hash_nghs.cpp
HEADERS := hash_nghs.h nghs_ht.h

TEST_TARGET := test_hash_nghs
BENCH_TARGET := benchmark_hash_nghs

.PHONY: all clean rebuild

all: $(TEST_TARGET) $(BENCH_TARGET)

$(TEST_TARGET): $(TEST_SRC) $(HEADERS)
	$(CXX) $(CXXFLAGS) $(TEST_SRC) -o $(TEST_TARGET)

$(BENCH_TARGET): $(BENCH_SRC) $(HEADERS)
	$(CXX) $(CXXFLAGS) $(BENCH_SRC) -o $(BENCH_TARGET)

# Build benchmark binaries for multiple HASH_B_SIZE values
BENCH_SIZES := 8 16 32 64 128
BENCH_BIN := $(patsubst %,benchmark_hash_nghs_b%,$(BENCH_SIZES))
$(BENCH_BIN): benchmark_hash_nghs_b%: $(BENCH_SRC) $(HEADERS)
	$(CXX) $(CXXFLAGS) -DHASH_B_SIZE=$* $(BENCH_SRC) -o $@

.PHONY: bench-all $(BENCH_BIN)

bench-all: $(BENCH_BIN)


rebuild: clean all

clean:
	rm -f $(TEST_TARGET) $(BENCH_TARGET) $(BENCH_BIN)
