#include "btree.hpp"
#include <iostream> 

btree::btree(){
	root = new page(LEAF);
	height = 1;
};

void btree::insert(char *key, uint64_t val){
	// Please implement this function in project 3.
}

uint64_t btree::lookup(char *key){
	// Please implement this function in project 3.
	uint64_t val = 0;
	return val;
}
