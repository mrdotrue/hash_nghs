#ifndef NEIGHBOR_HASH_RECORD
#define NEIGHBOR_HASH_RECORD
#include "parlay/parallel.h"
#include "parlay/sequence.h"
#include "parlay/utilities.h"
#include <atomic>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <parlay/primitives.h>
#include <sys/types.h>
#include <utility>
// For the hash table, we generate a 32-bit bitmap for every B kv pairs
template <uint32_t B = 16> class nghs_ht {
private:
  using key_t = uint32_t;
  using val_t = uint32_t;
  using index_t = key_t;
  struct entry_t {
    key_t k;
    val_t v;
    entry_t() : k(reserved_key), v(0) {}
    constexpr entry_t(key_t _k, val_t _v) : k(_k), v(_v) {}
    //  reserved entry
    static constexpr key_t reserved_key = std::numeric_limits<key_t>::max();
    static constexpr entry_t empty_entry{reserved_key, 0};
    static constexpr entry_t deleted_entry{reserved_key, 1};
    bool operator==(const entry_t &other) const {
      return k == other.k && v == other.v;
    }
    bool operator!=(const entry_t &other) const { return !(*this == other); }
  };

  static constexpr double load_factor = 0.75;
  static constexpr double expand_factor = 2.0;

  static constexpr key_t reserved_key = entry_t::reserved_key;

  key_t roommate;                    // level 1 edge
  entry_t *records;                  // hash table
  index_t capacity;                  // hash table capacity
  std::atomic<index_t> used_records; // used_records slots
  std::atomic<key_t> level_size[33]; // number of level i edges
  uint32_t *navigation;              // complete binary tree

  // iterate hash table
  index_t firstIndex(key_t k) const { return parlay::hash32(k) % capacity; }
  index_t incrementIndex(index_t h) {
    return ((h + 1) == capacity) ? 0 : h + 1;
  }
  index_t decrementIndex(index_t h) { return (h == 0) ? capacity - 1 : h - 1; }

  void ensure_capacity(uint32_t n_append) {
    if (capacity * load_factor < n_append + used_records) {
      entry_t *old_records = records;
      auto old_capacity = capacity;
      auto old_navigation = navigation;

      capacity = (std::max(n_append + used_records,
                           old_capacity * (index_t)expand_factor) /
                      B +
                  1) *
                 B;
      records = new entry_t[capacity];
      parlay::parallel_for(0, capacity,
                           [&](auto i) { records[i] = entry_t::empty_entry; });
      auto navigation_length = get_tree_size();
      // std::cout << navigation_length << std::endl;
      navigation = new uint32_t[navigation_length];
      memset(navigation, 0, navigation_length * sizeof(uint32_t));
      used_records = 0;
      for (auto i = 0; i < 33; i++)
        level_size[i] = 0;
      parlay::parallel_for(0, old_capacity, [&](auto i) {
        if (old_records[i] != entry_t::empty_entry ||
            old_records[i] != entry_t::deleted_entry)
          insert(old_records[i].k, old_records[i].v);
      });
      update_top_down();
      delete[] old_navigation;
      delete[] old_records;
    }
  }

  // atomic utils
  bool cas_64(entry_t *_ptr, entry_t _oval, entry_t _nval) {
    uint64_t *ptr = reinterpret_cast<uint64_t *>(_ptr);
    uint64_t oval = *(reinterpret_cast<uint64_t *>(&_oval));
    uint64_t nval = *(reinterpret_cast<uint64_t *>(&_nval));
    return __sync_bool_compare_and_swap(ptr, oval, nval);
  }
  uint32_t fetch_and_or(uint32_t *ptr, uint32_t nval) {
    return __sync_fetch_and_or(ptr, nval);
  }

  // complete bianry tree utils
  // make sure capacity is dividable by B
  uint32_t get_leaf_size() {
    assert(capacity % B == 0);
    return capacity / B;
  }
  uint32_t get_internal_node_size() { return get_leaf_size() - 1; }
  uint32_t get_tree_size() { return get_leaf_size() * 2 - 1; }
  uint32_t get_tree_index(index_t i) {
    return get_internal_node_size() + i / B;
  }
  static index_t get_left_child_id(index_t i) { return (i << 1) + 1; }
  static index_t get_right_child_id(index_t i) { return (i << 1) + 2; }
  bool isleaf(index_t k) { return k * 2 + 1 >= get_tree_size(); }
  static val_t get_bit_val(val_t l) {
    assert(l < 33);
    // l = 0 or 1 for empty/deleted entry
    return l > 1 ? (val_t)1 << (l - 1) : 0;
  }

  // update from bottom to top
  void update(index_t k) {
    index_t i = get_tree_index(k);
    // fetch_and_or(&navigation[i], 1);
    navigation[i] = 1;
    while (i) {
      i = (i - 1) / 2;
      if (navigation[i] == 1)
        return;
      // fetch_and_or(&navigation[i], 1);
      navigation[i] = 1;
    }
  }

  // work efficient update augmented value
  void update_top_down(index_t root = 0) {
    if (navigation[root] == 1) { // root got marked
      if (isleaf(root)) {
        // leaf in binary tree needs to be mapped to a block from hash table
        index_t start = (root - get_internal_node_size()) * B;
        navigation[root] = 0;
        for (auto i = start; i < start + B; i++)
          navigation[root] |= get_bit_val(records[i].v);
        return;
      }
      auto l = get_left_child_id(root);
      auto r = get_right_child_id(root);
      parlay::par_do(
          [&]() { // if (l < get_tree_size())
            update_top_down(l);
          },
          [&]() { // if (r < get_tree_size())
            update_top_down(r);
          });
      navigation[root] = navigation[l] | navigation[r];
    }
  }
  void fetch_top_down(parlay::sequence<key_t> &nghs, uint32_t level,
                      std::atomic<key_t> &fetched, index_t root = 0) {
    // level l should have lth bit set which is l - 1
    assert(root < get_tree_size());
    if ((navigation[root] & ((uint32_t)1 << (level - 1))) == 0)
      return;
    if (fetched >= nghs.size())
      return;
    if (isleaf(root)) {
      // leaf in binary tree needs to be mapped to a block from hash table
      index_t start = (root - get_internal_node_size()) * B;
      for (auto i = start; i < start + B; i++) {
        if (records[i].v == level) {
          auto o = fetched.fetch_add(1);
          if (o >= nghs.size())
            return;
          nghs[o] = records[i].k;
        }
      }
      return;
    }
    auto l = get_left_child_id(root);
    auto r = get_right_child_id(root);
    parlay::par_do(
        [&]() { // if (l < get_tree_size())
          fetch_top_down(nghs, level, fetched, l);
        },
        [&]() { // if (r < get_tree_size())
          fetch_top_down(nghs, level, fetched, r);
        });
  }
  //  batch insertion and batch deletion share the same insert function
  //  one can reach empty slot, delete slot or update record,
  //  need to maintain
  //  old_level counter, needed when update value
  //  new level counter, needed for all cases
  //  used_records, all but update
  //  update records, needed for all
  //  update bitmap, needed for all
  void insert(key_t k, val_t v) {
    // std::cout << k << " " << v << std::endl;
    if (v == 1) {
      // process level 1 edge
      if (!__sync_bool_compare_and_swap((uint32_t *)(&roommate),
                                        (uint32_t)reserved_key, (uint32_t)k)) {
        std::cout << "repeat inserting level 1 edge" << std::endl;
        std::abort();
      }
      level_size[1] = 1;
      remove(k, false); // k might already exist.
      return;
    }
    assert(k != reserved_key);
    assert(k != roommate); // level 1 edge can only be deleted
    index_t i = firstIndex(k);
    index_t st = i;
    entry_t item(k, v);
    while (true) {
      if (cas_64(&records[i], entry_t::deleted_entry, item) ||
          cas_64(&records[i], entry_t::empty_entry, item)) {
        used_records++;
        level_size[v]++;
        update(i);
        return;
      }
      i = incrementIndex(i);
      if (i == st) {
        std::cout << "hash table is full" << std::endl;
        std::abort();
      }
    }
  }
  void update(key_t k, val_t v) {
    if (k == roommate) // level 1 edge can only be deleted
      return;
    if (v == 1) {
      // process level 1 edge
      if (!__sync_bool_compare_and_swap((uint32_t *)(&roommate),
                                        (uint32_t)reserved_key, (uint32_t)k)) {
        std::cout << "repeat inserting level 1 edge" << std::endl;
        std::abort();
      }
      level_size[1] = 1;
      remove(k, false); // k might already exist.
      return;
    }
    index_t i = firstIndex(k);
    index_t st = i;
    while (records[i] != entry_t::empty_entry) {
      if (records[i].k == k) {
        level_size[records[i].v]--;
        assert(records[i].v != 0);
        level_size[v]++;
        records[i].v = v;
        update(i);
        return;
      }
      i = incrementIndex(i);
      if (i == st)
        break;
    }
    std::cout << "key doesn't exist" << std::endl;
    std::abort();
  }
  void remove(key_t k, bool check = true) {
    // std::cout << k << std::endl;
    // process level 1 edge
    if (k == roommate) {
      roommate = reserved_key;
      level_size[1] = 0;
      return;
    }
    index_t i = firstIndex(k);
    int st = i;
    while (records[i] != entry_t::empty_entry) {
      if (records[i].k == k) {
        assert(records[i].v != 0);
        level_size[records[i].v]--;
        records[i] = entry_t::deleted_entry; // mark slot as deleted
        update(i);
        used_records--;
        return;
      }
      i = incrementIndex(i);
      if (i == st)
        break;
    }
    if (check) {
      std::cout << "remove non-existent item" << std::endl;
      std::abort();
    }
  }
  val_t find(key_t k) {
    if (k == roommate)
      return 1;
    index_t i = firstIndex(k);
    index_t st = i;
    while (records[i] != entry_t::empty_entry) {
      if (records[i].k == k)
        return records[i].v;
      i = incrementIndex(i);
      if (i == st)
        break;
    }
    return 0;
  }

public:
  nghs_ht(index_t _capacity = B)
      : capacity((std::max(_capacity, (index_t)B) / B + 1) * B),
        used_records(0), roommate(/*_empty_key*/ reserved_key) {
    records = new entry_t[capacity];
    parlay::parallel_for(0, capacity,
                         [&](auto i) { records[i] = entry_t::empty_entry; });
    auto navigation_length = get_tree_size();
    // std::cout << navigation_length << std::endl;
    navigation = new uint32_t[navigation_length];
    memset(navigation, 0, navigation_length * sizeof(uint32_t));
    for (auto i = 0; i < 33; i++)
      level_size[i] = 0;
  }
  ~nghs_ht() {
    delete[] records;
    delete[] navigation;
  }
  nghs_ht(const nghs_ht &) = delete;
  nghs_ht &operator=(const nghs_ht &) = delete;
  nghs_ht(nghs_ht &&) = delete;
  nghs_ht &operator=(nghs_ht &&) = delete;

  index_t get_size() {
    index_t u = used_records;
    return (roommate == reserved_key) ? u : u + 1; //+1 for roommate
  }

  // batch insertion: sequence of (vertex, level)
  void batch_insertion(parlay::sequence<std::pair<key_t, val_t>> &ins) {
    ensure_capacity(ins.size());
    // std::cout << get_tree_size() << std::endl;
    parlay::parallel_for(0, ins.size(), [&](auto i) {
      insert(ins[i].first, ins[i].second);
      // assert(find(ins[i].first) == ins[i].second);
    });
    update_top_down();
  }
  // batch update: sequence of (vertex, level)
  void batch_update(parlay::sequence<std::pair<key_t, val_t>> &upd) {
    parlay::parallel_for(0, upd.size(), [&](auto i) {
      update(upd[i].first, upd[i].second);
      assert(find(upd[i].first) == upd[i].second);
    });
    update_top_down();
  }
  // batch deletion: only need to know the vertex
  void batch_deletion(parlay::sequence<key_t> &del) {
    parlay::parallel_for(0, del.size(), [&](auto i) {
      remove(del[i]);
      // assert(find(del[i]) == 0);
    });
    update_top_down();
  }
  parlay::sequence<val_t> batch_find(parlay::sequence<key_t> &K) {
    return parlay::map(K, [&](auto x) { return find(x); });
  }
  // fetch k level l edges
  parlay::sequence<key_t> fetch(key_t k, val_t l) {
    parlay::sequence<key_t> nghs;
    if (l == 1 && roommate != reserved_key)
      nghs.push_back(roommate);
    else {
      // if k > number of all level l edges, fetch all
      k = std::min(k, (key_t)level_size[l]);
      // store result in sequence, allocate space in advance
      nghs = parlay::sequence<key_t>(k);
      // use atomic variable to see how many edges we still need to fetch
      std::atomic<uint32_t> fetched = 0;
      fetch_top_down(nghs, l, fetched);
    }
    return nghs;
  }
  // debug export alive neighbors and their levels
  parlay::sequence<std::pair<key_t, val_t>> to_sequence_sorted() {
    parlay::sequence<std::pair<key_t, val_t>> alive;
    if (roommate != reserved_key)
      alive.emplace_back(std::pair(roommate, 1));
    for (index_t i = 0; i < capacity; i++) {
      if (records[i] != entry_t::empty_entry &&
          records[i] != entry_t::deleted_entry)
        alive.emplace_back(std::pair(records[i].k, records[i].v));
    }
    return parlay::remove_duplicates_ordered(
        alive,
        [&](const std::pair<key_t, val_t> &a,
            const std::pair<key_t, val_t> &b) { return a.first < b.first; });
  }
  // debug
  void print_level_size() {
    for (auto i = 0; i < 33; i++) {
      std::cout << "number of level " << i << " edges " << level_size[i]
                << std::endl;
    }
  }
  // debug check correctness for complete binary tree
  void print_tree_path(key_t k) {
    std::cout << "hash table capacity " << capacity << std::endl;
    index_t i = firstIndex(k);
    std::cout << "first index " << i << std::endl;
    index_t st = i;
    while (records[i] != entry_t::empty_entry) {
      if (records[i].k == k)
        break;
      i = incrementIndex(i);
      std::cout << "next index " << i << std::endl;
      if (i == st)
        break;
    }
    std::cout << "tree size " << get_tree_size() << std::endl;
    std::cout << "internal node size " << get_internal_node_size() << std::endl;
    std::cout << "leaf size " << get_leaf_size() << std::endl;
    auto x = get_tree_index(i) - get_internal_node_size();
    std::cout << "id of block " << x << std::endl;
    for (auto i = x * B; i < x * B + B; i++)
      std::cout << records[i].k << " " << records[i].v << std::endl;
    do {
      std::cout << x << " " << std::bitset<32>(navigation[x]) << std::endl;
      x = (x - 1) / 2;
    } while (x);
  }
  // print hash table layout
  void display() const {
    std::cout << "\n--- Hash record Contents (Size: " << used_records
              << " Capacity: " << capacity << ") ---" << std::endl;
    if (roommate != reserved_key)
      std::cout << "OCCUPIED (Key: " << roommate << ", Value: \"" << 1 << "\")";
    for (index_t i = 0; i < capacity; ++i) {
      std::cout << "[" << i << "]: ";
      if (records[i] != entry_t::empty_entry) {
        if (records[i] != entry_t::deleted_entry)
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
};
#endif