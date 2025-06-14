#include "btree.hpp"
#include <iostream>

btree::btree(){
	root = new page(LEAF);
	height = 1;
};

void btree::insert(char *key, uint64_t val){
	// Please implement this function in project 3.
	page *current = root;
	std::vector<std::pair<page *, int>> path;

	// 1. 노드 탐색
	while (current->get_type() == INTERNAL)
	{
    	path.push_back({current, 0});
    	uint64_t next = current->find(key);
    	current = reinterpret_cast<page *>(next);
	}

	char *parent_key = nullptr;
	page *new_node = nullptr;

	bool inserted = current->insert(key, val);
	if (!inserted) {
		page *target_leaf = current;
		page *new_leaf = current->split(key, val, &parent_key);
		target_leaf->print();
		new_leaf->print();
		new_node = new_leaf;
	}

	// 2. split
	while (!path.empty() && new_node != nullptr)
	{
		auto [parent, idx] = path.back();
		path.pop_back();

		if (!parent->insert(parent_key, (uint64_t)new_node)) {
			page *next_new = parent->split(parent_key, (uint64_t)new_node, &parent_key);
			new_node = next_new;
		} else {
			new_node = nullptr;
		}
	}

	// 3. root node가 split 필요한 경우
	if (new_node != nullptr) {
		page *new_root = new page(INTERNAL);
		new_root->set_leftmost_ptr(root);
		new_root->insert(parent_key, (uint64_t)new_node);
		root = new_root;
		height++;
	}
}

uint64_t btree::lookup(char *key){
	// Please implement this function in project 3.
	page* current = root;

	// 현재 root가 internal일 경우,
	while (current->get_type() == INTERNAL) {
	    uint64_t next = current->find(key);
	    current = reinterpret_cast<page*>(next);
	}
	uint64_t result = current->find(key);
	return result;
}
