# hash_nghs

## run example
```
git clone --recursive git@github.com:mrdotrue/hash_nghs.git

g++ -O3 -Iparlaylib/include -std=c++17 ./unit_test.cpp -o unit_test

./unit_test
```
## usage

```
#include "nghs_ht.h"

using key_t = uint32_t; //vertex_id
using val_t = uint32_t; //level

key_t n;
key_t capacity = 2 * n;
nghs_ht A(capacity);

// insert neighbors to different levels
A.batch_insertion(parlay::sequence<std::pair<key_t, val_t>> &ins)

// update the level of neighbors
A.batch_update(parlay::sequence<std::pair<key_t, val_t>> &upd)

// remove neighbors
A.batch_deletion(parlay::sequence<key_t> &del)

// retrieve neighbor level
parlay::sequence<val_t> result = A.batch_find(parlay::sequence<key_t> &K);

// fetch k level l edges;
parlay::sequence<key_t> fetched_edges = A.fetch(key_t k, val_t l);

std::cout << "total space used " << A.get_space_usage() / 1024 / 1024 << " MB"
            << std::endl;
```


