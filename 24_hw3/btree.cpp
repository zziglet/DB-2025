#include "btree.hpp"
#include <cstring>
#include <iostream>

// btree constructor
btree::btree() {
    root = new page(LEAF);
    height = 1;
}

void btree::insert(char* key, uint64_t val) {
    page* current = root;
    page** parents = new page*[height]; //부모 노드들을 다 탐색해봐야 함 -> 배열로 다 저장
    int parent_idx = 0;

    // 1. 넣을 수 있는 leaf node 찾기
    while (current->get_type() != LEAF) {
        parents[parent_idx++] = current;
        current = (page *)current->find(key);
    }
    
    //2. leaf node 저장 공간 확인 -> 꽉 차 있으면 split
    //record 크기만큼 -> record size(2byte = 16bit) + key 길이(null 포함)+ value(8byte = 64bit)
    uint16_t key_len = strlen(key);
    bool isFull = current->is_full(sizeof(uint16_t) + key_len + 1 + sizeof(uint64_t));
    if (isFull) { //만약 꽉 차 있다면 split
        char *new_key;
        page *new_node = current->split(key, val, &new_key);
       
       //2-1. 모든 부모 node 탐색
        for(int i = 0; i < parent_idx; i++){
            page *parent = parents[i];
            uint16_t parent_key_len = strlen(key);
            bool isFull_parent = parent->is_full(sizeof(uint16_t) + parent_key_len + 1 + sizeof(uint64_t));
            
            //2-1-1. 부모 노드도 꽉 찼다면 split
            if(isFull_parent){
                char *new_parent_key;
                page *new_nonleaf_page = parent->split(new_key, (uint64_t)new_node, &new_parent_key);
                new_node = new_nonleaf_page;
                new_key = new_parent_key;
            }else{
                parent->insert(new_key, (uint64_t)new_node);
                return;
            }
        }

        //2-2. 새로운 root 넣기 
        page *new_root = new page(INTERNAL);
        new_root->set_leftmost_ptr(root); //원래 root를 자식 노드로
        new_root->insert(new_key, (uint64_t)new_node); //새로 만든 node를 삽입
        
        //2-3. b+tree update
        this->root = new_root; 
        this->height++;

    }else{ //3. 빈 공간 있다면 그냥 insert
        current->insert(key,val);
    }
    current->print();
    delete[] parents;
}

uint64_t btree::lookup(char *key) {
    page* p = root;
    
    while (p->get_type() != LEAF) {
        p = (page*)(p -> find(key));
    }

    return (uint64_t)(p->find(key));
}
