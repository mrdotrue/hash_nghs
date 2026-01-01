// g++ -O3 -Iparlaylib/include -std=c++17 ./unit_test.cpp -o unit_test
#include "nghs_ht.h"
#include "parlay/internal/get_time.h"
#include "parlay/parallel.h"
#include "parlay/primitives.h"
#include "parlay/utilities.h"
#include <cassert>
#include <cstdint>
#include <string>
template <uint32_t B = 16> void basic_test(uint32_t n) {
  std::cout << "================================== start testing B = " << B
            << " =================================" << std::endl;
  // vertex id from 0,n-1
  // store u's nghs in a hashtable
  uint32_t u = parlay::hash32(n) % n;
  std::cout << "#vertices: " << n << ", vertex: " << u << std::endl;

  // we will insert n-1 elements into hash table
  // set the capacity to 2 * n;
  nghs_ht<B> A(2 * n);

  // create pairs for vertex id [0,u-1] + [u+1,n-1]
  // there shouldn't be edges from u to u
  // levels will be [2,32];
  auto vertices = parlay::tabulate(n - 1, [&](auto i) {
    return (i < u)
               ? std::pair((uint32_t)i, parlay::hash32((uint32_t)i) % 31 + 2)
               : std::pair((uint32_t)i + 1,
                           parlay::hash32((uint32_t)i + 1) % 31 + 2);
  });
  std::cout << "================= start batch insertion ================"
            << std::endl;
  parlay::internal::timer t_ins;
  A.batch_insertion(vertices);
  t_ins.next("batch insert " + std::to_string(n - 1) + " neighbors");

  std::cout << "================= correctness check ===================="
            << std::endl;
  auto res1 = A.to_sequence_sorted();
  assert(res1.size() == vertices.size());
  parlay::parallel_for(0, res1.size(), [&](auto i) {
    assert(parlay::hash32((uint32_t)res1[i].first) % 31 + 2 == res1[i].second);
  });
  std::cout << "passed!" << std::endl;

  std::cout << "================= start batch fetch ===================="
            << std::endl;
  for (uint32_t i = 2; i < 33; i++) {
    parlay::internal::timer t_fetch;
    auto result = A.fetch(n, i); // fetch all level i edges
    t_fetch.next("fetch level " + std::to_string(i) + " edges");
    // correctness check
    parlay::parallel_for(0, result.size(), [&](auto j) {
      if (parlay::hash32((uint32_t)result[j]) % 31 + 2 != i) {

        std::cout << result[j] << " "
                  << parlay::hash32((uint32_t)result[j]) % 31 + 2 << " " << i
                  << std::endl;
        A.print_tree_path(result[j]);
      }
      assert(parlay::hash32((uint32_t)result[j]) % 31 + 2 == i);
    });
    std::cout << result.size() << std::endl;
  }

  std::cout << "================= start batch deletion ================="
            << std::endl;
  // delete [0,n-1]
  auto deletions = parlay::tabulate(u, [&](auto i) { return (uint32_t)i; });
  parlay::internal::timer t_del;
  A.batch_deletion(deletions);
  t_del.next("remove " + std::to_string(u) + " edges ");

  std::cout << "================= correctness check ===================="
            << std::endl;
  auto res2 = A.to_sequence_sorted();
  parlay::parallel_for(0, res2.size(), [&](auto i) {
    assert(res2[i].first > u);
    assert(parlay::hash32((uint32_t)res2[i].first) % 31 + 2 == res2[i].second);
  });
  std::cout << "passed!" << std::endl;
  std::cout << "================= start batch fetch ===================="
            << std::endl;
  for (uint32_t i = 2; i < 33; i++) {
    parlay::internal::timer t_fetch;
    auto result = A.fetch(n, i); // fetch all level i edges
    t_fetch.next("fetch level " + std::to_string(i) + " edges");
    // correctness check
    parlay::parallel_for(0, result.size(), [&](auto j) {
      assert(parlay::hash32((uint32_t)result[j]) % 31 + 2 == i);
    });
    std::cout << result.size() << std::endl;
  }

  std::cout << "================= start batch update ==================="
            << std::endl;
  // updates level of [u+1,n] to 2
  auto updates = parlay::tabulate(n - u - 1, [&](auto i) {
    return std::pair((uint32_t)i + u + 1, (uint32_t)2);
  });
  parlay::internal::timer t_update;
  A.batch_update(updates);
  t_update.next("update " + std::to_string(u) + " edges ");

  std::cout << "================= correctness check ===================="
            << std::endl;
  auto res3 = A.to_sequence_sorted();
  assert(res3.size() == updates.size());
  parlay::parallel_for(0, res3.size(), [&](auto i) {
    assert(res3[i].first > u);
    assert(2 == res3[i].second);
  });
  std::cout << "passed!" << std::endl;

  std::cout << "================= start batch find ====================="
            << std::endl;
  auto alive =
      parlay::tabulate(n - u - 1, [&](auto i) { return (uint32_t)i + u + 1; });
  parlay::internal::timer t_find;
  auto res4 = A.batch_find(alive);
  t_find.next("batch find ");
  parlay::parallel_for(0, res4.size(), [&](auto i) { assert(res4[i] == 2); });
  std::cout << "passed!" << std::endl;
}
int main() {
  // basic_test(1024);
  basic_test(1024 * 1024);
  basic_test(1024 * 1024 * 10);
  // basic_test<32>(1024);
  basic_test<32>(1024 * 1024);
  basic_test<32>(1024 * 1024 * 10);
  // basic_test<64>(1024);
  basic_test<64>(1024 * 1024);
  basic_test<64>(1024 * 1024 * 10);
  // basic_test(1024 * 1024 * 100);
  return 0;
}