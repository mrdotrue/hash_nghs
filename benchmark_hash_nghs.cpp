#include "nghs_ht.h"
#include "parlay/parallel.h"
#include "parlay/random.h"
#include "parlay/utilities.h"
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <sys/types.h>
#include <utility>
template <uint32_t B>
void bench_insert(nghs_ht<B> &ht,
                  parlay::sequence<std::pair<uint32_t, uint32_t>> &A,
                  uint32_t k = 1 << 16) {}
template <uint32_t B> void bench_fetch(nghs_ht<B> &ht, uint32_t k = 1 << 16) {}
template <uint32_t B>
void bench_deletion(nghs_ht<B> &ht,
                    parlay::sequence<std::pair<uint32_t, uint32_t>> &A,
                    uint32_t k = 1 << 16) {}
template <uint32_t B = 8> void bench_n(uint32_t n = 1 << 16) {
  parlay::sequence<std::pair<uint32_t, uint32_t>> A(n);
  parlay::sequence<uint32_t> _A(n);
  parlay::parallel_for(0, n, [&](auto i) {
    A[i] = std::pair((uint32_t)i, (parlay::hash32((uint32_t)i) % 31) + 2);
    _A[i] = (uint32_t)i;
  });
  std::cout << "================= N = " << n << " B = " << B
            << " =================" << std::endl;
  A = parlay::random_shuffle(A);
  _A = parlay::random_shuffle(_A);
  parlay::internal::timer t_ins;
  nghs_ht<B> H(n, n * 2);
  H.batch_insertion(A);
  t_ins.next("insertion");
  uint32_t k = 1000;
  for (auto i = 0; i < 5; i++) {
    k *= 10;
    for (auto l = 2; l < 33; l++) {
      parlay::internal::timer t;
      H.fetch(k, l);
      t.next("fetch " + std::to_string(k) + " level " + std::to_string(l) +
             " edges");
    }
  }
  //   H.print_level_size();
  parlay::internal::timer t_del;
  H.batch_deletion(_A);
  t_del.next("deletion");
}
int main() {
#ifdef HASH_B_SIZE
  // bench_n<HASH_B_SIZE>(100000);
  // bench_n<HASH_B_SIZE>(1000000);
  // bench_n<HASH_B_SIZE>(10000000);
  bench_n<HASH_B_SIZE>(100000000);
#endif
  return 0;
}