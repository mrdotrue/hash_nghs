#ifndef NEIGHBOR_HASH_record
#define NEIGHBOR_HASH_record
#include "parlay/parallel.h"
#include "parlay/sequence.h"
#include "parlay/utilities.h"
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <parlay/primitives.h>
#include <sys/types.h>
#include <utility>
template <uint32_t B = 16> class nghs_ht {
private:
  using key_t = uint32_t;
  using val_t = uint32_t;
  using index_t = key_t;
  struct entry_t {
    // v == 0 means empty_key
    // v == 1 means deleted
    // otherwise occupied
    key_t k;
    val_t v;
    entry_t(){};
    inline static val_t get_val(val_t l) {
      // convert l in [1,32] to 2^(l-1);
      assert(l > 0);
      assert(l < 33);
      return (val_t)1 << (l - 1);
    }
    inline static val_t get_raw(val_t v) {
      assert(v != 0);
      return __builtin_ctz(v) + 1;
    }
    entry_t(key_t _k, val_t _v) : k(_k), v(get_val(_v)){};
    entry_t(key_t _k) : k(_k), v(0){};
    bool operator==(const entry_t &other) const {
      return k == other.k && v == other.v;
    }
    bool operator!=(const entry_t &other) const { return !(*this == other); }
  };
  static constexpr val_t empty_val = 0;
  static constexpr val_t deleted_val = 1;
  static constexpr double load_factor = 0.75;
  static constexpr double expand_factor = 2.0;

  entry_t empty_entry;
  entry_t deleted_entry;
  key_t empty_key;
  key_t roommate; // level 1 edge
  entry_t *records;
  std::atomic<index_t> used;
  parlay::sequence<uint32_t> navigation;
  index_t capacity;
  index_t firstIndex(key_t k) const { return parlay::hash32(k) % capacity; }
  index_t incrementIndex(index_t h) {
    return ((h + 1) == capacity) ? 0 : h + 1;
  }
  index_t decrementIndex(index_t h) { return (h == 0) ? capacity - 1 : h - 1; }
  inline size_t get_leaf_size(index_t capacity) { return capacity / B; }
  inline size_t get_tree_size(index_t capacity) {
    return (size_t)capacity / B * 2 - 1;
  }
  inline size_t get_tree_index(index_t i) { return capacity / B - 1 + i / B; }
  void initialization(entry_t *records, index_t capacity, entry_t empty_entry) {
    parlay::parallel_for(0, capacity,
                         [&](auto i) { records[i] = empty_entry; });
  }
  void ensure_capacity(uint32_t n_append) {
    if (0.75 * capacity < n_append + used) {
      uint32_t new_capacity =
          std::min(std::max(n_append + used, capacity * 2), (index_t)1 << 31) /
          B * B;
      entry_t *new_records = new entry_t[new_capacity];
      initialization(new_records, new_capacity, empty_entry);
      entry_t *old_records = records;
      records = new_records;
      index_t old_capacity = capacity;
      capacity = new_capacity;
      navigation = parlay::sequence<uint32_t>(get_tree_size(capacity), 0);
      parlay::parallel_for(0, old_capacity, [&](auto i) {
        if (old_records[i] != deleted_entry && old_records[i] != empty_entry)
          insert(old_records[i].k, entry_t::get_raw(old_records[i].v));
      });
      delete[] old_records;
    }
  }
  bool cas(entry_t *_ptr, entry_t _oval, entry_t _nval) {
    uint64_t *ptr = reinterpret_cast<uint64_t *>(_ptr);
    uint64_t oval = *(reinterpret_cast<uint64_t *>(&_oval));
    uint64_t nval = *(reinterpret_cast<uint64_t *>(&_nval));
    return __sync_bool_compare_and_swap(ptr, oval, nval);
  }

public:
  nghs_ht(key_t _empty_key, index_t _capacity = B)
      : capacity(std::max(_capacity, (index_t)B) / B * B), used(0),
        empty_key(_empty_key), empty_entry(entry_t(_empty_key)),
        roommate(_empty_key),
        // navigation(parlay::sequence<uint32_t>(get_tree_size(capacity), 0)),
        deleted_entry(entry_t(_empty_key, deleted_val)) {
    records = new entry_t[capacity];
    initialization(records, capacity, empty_entry);
  }
  ~nghs_ht() { delete[] records; }
  nghs_ht(const nghs_ht &) = delete;
  nghs_ht &operator=(const nghs_ht &) = delete;
  nghs_ht(nghs_ht &&) = delete;
  nghs_ht &operator=(nghs_ht &&) = delete;

public:
  index_t get_size() {
    return used + 1; //+1 for roommate
  }
  void insert_exclusive(key_t k, val_t v) {
    if (v == 1) {
      if (roommate != empty_key) {
        std::cout << "repeat inserting level 1 edge" << std::endl;
        std::abort();
      }
      roommate = k;
      return;
    }
    if (k == roommate) {
      roommate = empty_key;
    }
    // std::cout << k << " " << v << std::endl;
    index_t i = firstIndex(k);
    index_t st = i;
    entry_t item(k, v);
    while (records[i] != empty_entry) {
      if (records[i].k == k) {
        // update value
        records[i] = item;
        return;
      }
      if (records[i] == deleted_entry) {
        // find a deleted slot
        records[i] = item;
        used++;
        return;
      }
      i = incrementIndex(i);
      if (i == st) {
        std::cout << "hash table is full" << std::endl;
        std::abort();
      }
    }
    records[i] = item;
    used++;
  }

  void insert(key_t k, val_t v) {
    // std::cout << k << " " << v << std::endl;
    if (v == 1) {
      if (!__sync_bool_compare_and_swap((uint32_t *)(&roommate),
                                        (uint32_t)empty_key, (uint32_t)k)) {
        std::cout << "repeat inserting level 1 edge" << std::endl;
        std::abort();
      }
      return;
    }
    assert(k != empty_key);
    assert(k != roommate); // level 1 edge cannot be updated
    index_t i = firstIndex(k);
    index_t st = i;
    entry_t item(k, v);
    while (true) {
      if (records[i].k == k) {
        // update value
        records[i] = item;
        return;
      }
      if (cas(&records[i], deleted_entry, item) ||
          cas(&records[i], empty_entry, item)) {
        used++;
        return;
      }
      i = incrementIndex(i);
      if (i == st) {
        std::cout << "hash table is full" << std::endl;
        std::abort();
      }
    }
  }

  val_t find(key_t k) {
    if (k == roommate)
      return 1;
    assert(k != empty_key);
    index_t i = firstIndex(k);
    index_t st = i;
    while (records[i] != empty_entry) {
      if (records[i].k == k)
        return entry_t::get_raw(records[i].v);
      i = incrementIndex(i);
      if (i == st)
        break;
    }
    return 0;
  }

  void remove(key_t k) {
    // std::cout << k << std::endl;
    if (k == roommate) {
      roommate = empty_key;
      return;
    }
    index_t i = firstIndex(k);
    int st = i;
    while (records[i] != empty_entry) {
      if (records[i].k == k) {
        records[i] = deleted_entry; // mark slot as deleted
        used--;
        return;
      }
      i = incrementIndex(i);
      if (i == st)
        break;
    }
    std::cout << "remove non-existent item" << std::endl;
    std::abort();
  }

  void display() const {
    std::cout << "\n--- Hash record Contents (Size: " << used
              << " Capacity: " << capacity << ") ---" << std::endl;
    if (roommate != empty_key)
      std::cout << "OCCUPIED (Key: " << roommate << ", Value: \"" << 1 << "\")";
    for (index_t i = 0; i < capacity; ++i) {
      std::cout << "[" << i << "]: ";
      if (records[i] != empty_entry) {
        if (records[i] != deleted_entry)
          std::cout << "OCCUPIED (Key: " << records[i].k << ", Value: \""
                    << records[i].v << "\")";
        else
          std::cout << "DELETED";
      } else {
        std::cout << "empty_key";
      }
      std::cout << std::endl;
    }
    std::cout << "------------------------------------------" << std::endl;
  }
  parlay::sequence<std::pair<key_t, val_t>> to_sequence_sorted() {
    parlay::sequence<std::pair<key_t, val_t>> alive;
    if (roommate != empty_key)
      alive.emplace_back(std::pair(roommate, 1));
    for (index_t i = 0; i < capacity; i++) {
      if (records[i] != empty_entry && records[i] != deleted_entry)
        alive.emplace_back(
            std::pair(records[i].k, entry_t::get_raw(records[i].v)));
    }
    parlay::sort_inplace(alive, [&](const std::pair<key_t, val_t> &a,
                                    const std::pair<key_t, val_t> &b) {
      return a.first < b.first;
    });
    return alive;
  }
};
#endif