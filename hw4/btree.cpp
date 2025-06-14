#include "btree.hpp"
#include <iostream>
#include <vector>
#include <utility>

btree::btree()
{
	root = new page(LEAF);
	height = 1;
};

void btree::insert(char *key, uint64_t val)
{
	// Thread-safe insert with latch coupling
	// lock 얻기 실패할 때마다 재시도 => loop
	while (true) { 
		page *current = root;
		std::vector<std::pair<page *, uint64_t>> path;
		bool restart = false;

		// 1. Navigate to leaf with latch coupling
		while (current->get_type() == INTERNAL && !restart)
		{
			// Read current page version
			uint64_t curr_version = current->read_version();

			// Try to acquire read lock
			if (!current->try_read_lock(curr_version))
			{
				restart = true;
				break;
			}

			// Add to path with version
			path.push_back({current, curr_version});

			// Find next page
			uint64_t next = current->find(key);
			page *next_page = reinterpret_cast<page *>(next);

			// Get next page version and try to lock it
			uint64_t next_version = next_page->read_version();
			if (!next_page->try_read_lock(next_version))
			{
				// Failed to lock next page, release current and restart
				restart = true;
				break;
			}

			// Successfully locked next page, can release current
			if (!current->read_unlock(curr_version))
			{
				// Current page was modified, restart
				restart = true;
				break;
			}

			current = next_page;
		}

		if (restart)
		{
			// Release all locks in path
			for (auto it = path.rbegin(); it != path.rend(); ++it)
			{
				it->first->read_unlock(it->second);
			}
			continue; // Restart from beginning
		}

		// 2. Now at leaf page, try to acquire write lock
		if (!current->try_write_lock())
		{
			// Failed to acquire write lock, release path locks and restart
			for (auto it = path.rbegin(); it != path.rend(); ++it)
			{
				it->first->read_unlock(it->second);
			}
			continue;
		}

		// 3. Try to insert into leaf
		char *parent_key = nullptr;
		page *new_node = nullptr;

		bool inserted = current->insert(key, val);
		// Need to split
		if (!inserted) {
			page *target_leaf = current;
			page *new_leaf = current->split(key, val, &parent_key);
			target_leaf->print();
			new_leaf->print();
			new_node = new_leaf;
		}

		// Release leaf write lock
		current->write_unlock();

		// 4. Handle splits propagating upward
		while (!path.empty() && new_node != nullptr)
		{
			auto [parent, parent_version] = path.back();
			path.pop_back();

			// Try to acquire write lock on parent
			if (!parent->try_write_lock())
			{
				// Failed to acquire write lock, restart entire operation
				for (auto it = path.rbegin(); it != path.rend(); ++it)
				{
					it->first->read_unlock(it->second);
				}
				restart = true;
				break;
			}

			// Validate parent hasn't changed
			if (!parent->validate_read(parent_version))
			{
				// Parent was modified, restart
				parent->write_unlock();
				for (auto it = path.rbegin(); it != path.rend(); ++it)
				{
					it->first->read_unlock(it->second);
				}
				restart = true;
				break;
			}

			// Try to insert into parent
			if (!parent->insert(parent_key, (uint64_t)new_node))
			{
				// Parent needs to split
				page *next_new = parent->split(parent_key, (uint64_t)new_node, &parent_key);
				new_node = next_new;
			} else {
				new_node = nullptr;
			}

			// Release parent write lock
			parent->write_unlock();
		}

		if (restart) {
			continue; // Restart from beginning
		}

		// 5. Handle root split if needed
		if (new_node != nullptr) {
			// Need to create new root - this requires global tree lock
			// For simplicity, we'll use a simple approach here
			page *new_root = new page(INTERNAL);
			new_root->set_leftmost_ptr(root);
			new_root->insert(parent_key, (uint64_t)new_node);
			root = new_root;
			height++;
		}

		// Release remaining path locks
		for (auto it = path.rbegin(); it != path.rend(); ++it)
		{
			it->first->read_unlock(it->second);
		}

		break; // Successfully completed
	}
}

uint64_t btree::lookup(char *key)
{
	// Thread-safe lookup with optimistic locking

	while (true) { // Retry loop
		page *current = root;
		uint64_t result = 0;
		bool restart = false;

		// Navigate to leaf with version validation
		while (current->get_type() == INTERNAL && !restart) {
			uint64_t version = current->read_version();

			// Check if page is stable for reading
			if (!current->try_read_lock(version)) {
				restart = true;
				break;
			}

			// Find next page
			uint64_t next = current->find(key);

			// Validate page hasn't changed
			if (!current->validate_read(version)) {
				restart = true;
				break;
			}

			current = reinterpret_cast<page*>(next);
		}

		if (restart) {
			continue; // Restart from root
		}

		// At leaf page, perform lookup with version validation
		uint64_t leaf_version = current->read_version();

		// Check if leaf is stable for reading
		if (!current->try_read_lock(leaf_version)) {
			continue; // Restart
		}

		// Perform the actual lookup
		result = current->find(key);

		// Validate leaf hasn't changed during lookup
		if (!current->validate_read(leaf_version)) {
			continue; // Restart
		}

		return result;
	}
}