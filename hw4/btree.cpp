#include "btree.hpp"
#include <iostream>
#include <thread> 

// path tracking을 위한 stack 구조 구현 -> <vector> 클래스 대체
struct PathNode {
	page *page_ptr;
	uint64_t version;
};

struct PathStack {
	PathNode nodes[64]; // maximum height
	int top;

	PathStack() : top(-1) {}

	void push(page *p, uint64_t v) {
		if (top < 63)
		{
			nodes[++top] = {p, v};
		}
	}

	PathNode pop() {
		if (top >= 0)
		{
			return nodes[top--];
		}
		std::cerr << "[ERROR] PathStack underflow!\n";
		return {nullptr, 0};
	}

	bool empty() {
		return top < 0;
	}

	void clear() {
		top = -1;
	}

	// Release all locks
	void release_all_read_locks() {
		for (int i = top; i >= 0; i--)
		{
			nodes[i].page_ptr->read_unlock(nodes[i].version);
		}
		clear();
	}

	int size(){
		return top + 1;
	}
};

btree::btree() {
	root = new page(LEAF);
	height = 1;
};

void btree::insert(char *key, uint64_t val) {
	
	// Thread-safe insert with latch coupling
	// lock 얻기 실패할 때마다 재시도 => loop
	std::cout << "[INSERT] Starting insert for key: " << key << ", val: " << val << std::endl;
	while (true) { 
		page *current = root;
		PathStack path;
		bool restart = false;

		std::cout << "[INSERT] Starting traversal from root" << std::endl;
		// 1. leaf page 탐색 (latch coupling
		while (current->get_type() == INTERNAL && !restart)
		{
			// 현재 version 읽고
			uint64_t curr_version = current->read_version();
			std::cout << "[INSERT] At internal node, version: " << curr_version << std::endl;

			// 그 version으로 read_lock 시도
			if (!current->try_read_lock(curr_version)) {
				// lock 얻기 실패, 다시 시작
				std::cout << "[INSERT] Failed to acquire read lock, restarting" << std::endl;
				restart = true;
				break;
			}

			// 현재 version으로 read_lock 성공하면 path에 추가해야 함
			path.push(current, curr_version);
			std::cout << "[INSERT] Added to path, depth: " << path.size() << std::endl;

			// 다음 page 찾기
			uint64_t next = current->find(key);

			if (next == 0) {
			    std::cerr << "[ERROR] Invalid child pointer (0) returned by find(). Restarting.\n";
			    restart = true;
			    break;
			}

			page *next_page = reinterpret_cast<page *>(next);

			if (reinterpret_cast<uintptr_t>(next_page) % alignof(page) != 0) {
			    std::cerr << "[ERROR] Misaligned page pointer returned by find(). Restarting.\n";
			    restart = true;
			    break;
			}

			if (!current->read_unlock(curr_version)) {
				std::cout << "[INSERT] Current page modified during unlock, restarting" << std::endl;
				restart = true;
				break;
			}

			uint64_t next_version = next_page->read_version();
			if (!next_page->try_read_lock(next_version)) {
				std::cout << "[INSERT] Failed to lock next page, restarting" << std::endl;
				restart = true;
				break;
			}

			current = next_page;
		}

		if (restart) {
			// restart할 때는 path에 있는 모든 read lock을 해제해야 함
			path.release_all_read_locks();
			std::cout << "[INSERT] Restarting entire traversal" << std::endl;
			continue;
		}

		std::cout << "[INSERT] Reached leaf node" << std::endl;
		// 2. leaf page에 도달했으므로 write_lock 시도
		int retry = 0;
		while (!current->try_write_lock())
		{
			std::cout << "[INSERT] Failed to acquire write lock on leaf, retry: " << retry << std::endl;
			path.release_all_read_locks();
			return; // 혹은 break;
		}

		std::cout << "[INSERT] Acquired write lock on leaf" << std::endl;

		// 3. write_lock 얻기 성공, leaf page에서 insert 시도
		char *parent_key = nullptr;
		page *new_node = nullptr;

		bool inserted = current->insert(key, val);
		
		// 기존 split이 필요한 경우 처리
		if (!inserted) {
			std::cout << "[INSERT] Leaf full, splitting..." << std::endl;
			page *target_leaf = current;
			page *new_leaf = current->split(key, val, &parent_key);
			// Null check after split
			if (!new_leaf) {
			    std::cerr << "[ERROR] Split failed: new_leaf is nullptr\n";
			    current->write_unlock();
			    path.release_all_read_locks();
			    restart = true;
			    break;
			}
			if (new_leaf) {
				std::cout << "[INSERT] Split completed" << std::endl;
				target_leaf->print();
				new_leaf->print();
				new_node = new_leaf;
			}
		} else {
			std::cout << "[INSERT] Successfully inserted into leaf" << std::endl;
		}

		// insert 성공 -> write_unlock
		current->write_unlock();
		std::cout << "[INSERT] Released write lock on leaf" << std::endl;

		// Stabilize the version after split (helps avoid future write lock failures)
		std::this_thread::sleep_for(std::chrono::microseconds(50));

		// 4. 부모 노드로 올라가면서 split 처리
		while (!path.empty() && new_node != nullptr) {
			PathNode parent_info = path.pop();
			page *parent = parent_info.page_ptr;

			if (!parent) {
			    std::cerr << "[ERROR] Path pop returned null page pointer. Aborting insert.\n";
			    restart = true;
			    break;
			}

			uint64_t parent_version = parent_info.version;

			// Re-validate current page after split
			uint64_t split_version = current->read_version();
			if (!current->try_read_lock(split_version) || !current->validate_read(split_version)) {
				restart = true;
				path.release_all_read_locks();
				break;
			}
			current->read_unlock(split_version);

			std::cout << "[INSERT] Propagating split to parent level" << std::endl;

			// 부모 노드에 대한 write_lock 시도
			if (!parent->try_write_lock()) {
				// write_lock 실패, restart
				std::cout << "[INSERT] Failed to acquire write lock on parent, need re-traversal" << std::endl;
				path.release_all_read_locks();
				restart = true;
				break;
			}

			// 부모 노드 버전으로 검증
			if (!parent->validate_read(parent_version)) {
				// 수정된 경우, write_unlock 후 restart
				std::cout << "[INSERT] Parent modified, restarting" << std::endl;
				parent->write_unlock();
				path.release_all_read_locks();
				restart = true;
				break;
			}

			// 부모 노드에 새 노드 삽입 시도
			if (!new_node) {
			    std::cerr << "[ERROR] Cannot insert nullptr new_node into parent\n";
			    parent->write_unlock();
			    restart = true;
			    break;
			}
			if (!parent->insert(parent_key, reinterpret_cast<uint64_t>(new_node))) {
				// Parent needs to split
				std::cout << "[INSERT] Parent also needs split" << std::endl;
				page *next_new = parent->split(parent_key, reinterpret_cast<uint64_t>(new_node), &parent_key);
				new_node = next_new;
			} else {
				std::cout << "[INSERT] Successfully inserted into parent" << std::endl;
				new_node = nullptr;
			}

			// 부모 노드 write_unlock
			parent->write_unlock();
		}

		if (restart) {
			std::cout << "[INSERT] Restarting due to parent lock failure" << std::endl;
			continue;
		}

		// 5. root 노드가 split이 필요한 경우 처리
		if (new_node != nullptr) {
			// 새로운 root 노드 생성
			std::cout << "[INSERT] Creating new root due to root split" << std::endl;
			page *new_root = new page(INTERNAL);
			new_root->set_leftmost_ptr(root);
			new_root->insert(parent_key, reinterpret_cast<uint64_t>(new_node));
			root = new_root;
			height++;
			std::cout << "[INSERT] New root created, height: " << height << std::endl;
		}

		// path에 있는 모든 read lock 해제
		path.release_all_read_locks();

		std::cout << "[INSERT] Insert completed successfully" << std::endl;
		break;
	}
}

uint64_t btree::lookup(char *key)
{
	// Thread-safe lookup with optimistic locking
	// lock 얻기 실패할 때마다 재시도 => loop
	std::cout << "[LOOKUP] Starting lookup for key: " << key << std::endl;
	while (true) {
		page *current = root;
		uint64_t result = 0;
		bool restart = false;
		
		// root에서 leaf page로 이동 + version 검증
		while (current->get_type() == INTERNAL && !restart) {
			uint64_t version = current->read_version();
			std::cout << "[LOOKUP] At internal node, version: " << version << std::endl;

			// read_lock 시도
			if (!current->try_read_lock(version)) {
				std::cout << "[LOOKUP] Page not stable for reading, restarting" << std::endl;
				restart = true;
				break;
			}

			// 다음 page 찾기
			uint64_t next = current->find(key);

			// page 변경 여부 체크
			if (!current->validate_read(version)) {
				std::cout << "[LOOKUP] Page changed during traversal, restarting" << std::endl;
				current->read_unlock(version);
				restart = true;
				break;
			}

			current->read_unlock(version);

			current = reinterpret_cast<page*>(next);
		}

		if (restart){
			std::cout << "[LOOKUP] Restarting from root" << std::endl;
			continue;
		}

		// leaf page 도달, read_version 읽기
		std::cout << "[LOOKUP] Reached leaf node" << std::endl;
		uint64_t leaf_version = current->read_version();
		int retry = 0;
		while (!current->try_read_lock(leaf_version)) {
			leaf_version = current->read_version(); // re-read version
		}

		// lookup 수행
		result = current->find(key);

		// look up 도중에 변경된 사항 있는지 체크
		if (!current->validate_read(leaf_version)) {
			std::cout << "[LOOKUP] Leaf changed during lookup, restarting" << std::endl;
			current->read_unlock(leaf_version);
			continue; // Restart
		}

		current->read_unlock(leaf_version);

		std::cout << "[LOOKUP] Found result: " << result << std::endl;
		return result;
	}
}