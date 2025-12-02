#include "hash_nghs.h"
#include "nghs_ht.h"
#include "parlay/parallel.h"
#include "parlay/utilities.h"
#include <cassert>
#include <cstdint>
#include <cstdlib>
void test1() {
  std::cout << "test 1" << std::endl;
  hashtable<hash_numeric<int>> table(400000, hash_numeric<int>{});
  ;

  parlay::parallel_for(1, 100000, [&](int i) { table.insert(i); });

  parlay::parallel_for(1, 100000, [&](int i) {
    auto val = table.find(i);
    if (val != i)
      std::abort();
  });
}
void test_nghs_ht_sequential(uint32_t n = 128) {
  nghs_ht ht(n + 1, n * 2);
  for (uint32_t i = 0; i < n; i++) {
    if (i % 2)
      ht.insert_exclusive(i, parlay::hash32(i) % 31 + 2);
  }
  for (auto i = 0; i < n; i++) {
    if (i % 2) {
      assert(ht.find(i) == parlay::hash32(i) % 31 + 2);
    } else {
      assert(ht.find(i) == 0);
    }
  }
  for (uint32_t i = 0; i < n; i++) {
    if (i % 3 && i % 2)
      ht.remove(i);
  }
  for (auto i = 0; i < n; i++) {
    if (i % 2 && i % 3 == 0) {
      assert(ht.find(i) == parlay::hash32(i) % 31 + 2);
    } else {
      assert(ht.find(i) == 0);
    }
  }
  ht.display();
  auto alive = ht.to_sequence_sorted();
  std::cout << alive.size() << std::endl;
  for (auto it : alive)
    std::cout << it.first << " " << it.second << std::endl;
}
void test_nghs_ht_parallel(uint32_t n = 1024 * 1024 * 16) {
  nghs_ht ht(n + 1, n * 2);
  parlay::parallel_for(0, n, [&](auto i) {
    if (i % 2)
      ht.insert(i, parlay::hash32(i) % 31 + 2);
  });
  parlay::parallel_for(0, n, [&](auto i) {
    if (i % 2) {
      assert(ht.find(i) == parlay::hash32(i) % 31 + 2);
    } else {
      assert(ht.find(i) == 0);
    }
  });
  parlay::parallel_for(0, n, [&](auto i) {
    if (i % 3 && i % 2)
      ht.remove(i);
  });
  parlay::parallel_for(0, n, [&](auto i) {
    if (i % 2 && i % 3 == 0) {
      assert(ht.find(i) == parlay::hash32(i) % 31 + 2);
    } else {
      assert(ht.find(i) == 0);
    }
  });
  parlay::parallel_for(0, n, [&](auto i) {
    if (i % 3 && i % 2)
      ht.insert(i, parlay::hash32(i) % 31 + 2);
  });
  parlay::parallel_for(0, n, [&](auto i) {
    if (i % 2) {
      assert(ht.find(i) == parlay::hash32(i) % 31 + 2);
    } else {
      assert(ht.find(i) == 0);
    }
  });
  // ht.display();
  auto alive = ht.to_sequence_sorted();
  std::cout << alive.size() << std::endl;
  // for (auto it : alive)
  //   std::cout << it.first << " " << it.second << std::endl;
}
int main() {

  // test_nghs_ht_sequential();
  test_nghs_ht_parallel();
  return 0;
}