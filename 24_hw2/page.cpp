#include "page.hpp"
#include <iostream> 
#include <cstring> 

void put2byte(void *dest, uint16_t data){
	*(uint16_t*)dest = data;
}

uint16_t get2byte(void *dest){
	return *(uint16_t*)dest;
}

page::page(uint16_t type){
	hdr.set_num_data(0); //data 전체 개수를 0으로
	hdr.set_data_region_off(PAGE_SIZE-1-sizeof(page*)); //data offset 끝
	hdr.set_offset_array((void*)((uint64_t)this+sizeof(slot_header)));
	hdr.set_page_type(type); 

}

uint16_t page::get_type(){
	return hdr.get_page_type();
}

uint16_t page::get_record_size(void *record){
	uint16_t size = *(uint16_t *)record;
	return size;
}

char *page::get_key(void *record){
	char *key = (char *)((uint64_t)record+sizeof(uint16_t));
	return key;
}

uint64_t page::get_val(void *key){
	uint64_t val= *(uint64_t*)((uint64_t)key+(uint64_t)strlen((char*)key)+1);
	return val;
}

void page::set_leftmost_ptr(page *p){
	leftmost_ptr = p;
}

page *page::get_leftmost_ptr(){
	return leftmost_ptr;	
}

uint64_t page::find(char *key){
	// Please implement this function in project 2.

	//1. (hd) num_records 개수 찾기
	//2. (hd) record offset arr -> record 개수만큼 다 뒤져보기
	//3. (data) record 찾아서 key 값 비교
	//4. (data) 동일한 key라면 그 key의 value 가지고 옴

	uint64_t val = 0;
	void *offset_arr = hdr.get_offset_array();

	for(int i=0; i < hdr.get_num_data(); i++){
		uint16_t offsetarr_off = *(uint16_t *)((uint64_t)offset_arr+i*2);	
		void *record_ptr = (void *)((uint64_t)this + (uint64_t)offsetarr_off);

		char *record_key = get_key(record_ptr);
		if(strcmp(record_key, key) == 0){
			val = get_val(record_key);
		}
	}

	return val;
}

bool page::insert(char *key,uint64_t val){
	// Please implement this function in project 2.

	//insert
	//1. is_full() 함수로 space 공간 확인
	//2. (data) 저장할 위치(record_offset) 구하기 (data_region_off - 현재 record size)
	//3. (hd) offset_arr에 record_offset 정렬 및 위치 구하기 -> key 값으로 offset_arr 정렬되도록, 모든 key 확인 후 정렬
	//4. 모든 소스들 update
	//4-1. (data) record 삽입 -> size, key, value
	//4-2. (hd) record_offset 추가
	//4-3  (hd) data_region_off 정렬
	//4-2. (hd) num_data ++ 
	
	uint16_t key_len = strlen(key);
	//record 크기만큼 -> record size(2byte = 16bit) + key 길이(null 포함)+ value(8byte = 64bit)
	uint16_t record_size = sizeof(uint16_t) + key_len + 1 + sizeof(uint64_t);

	//1. is full 확인
	if(is_full(record_size)){
		return false;
	}

	//2. 저장할 위치 구하기
	uint16_t record_offset = hdr.get_data_region_off() - record_size;
	
	//만약에 첫번째 insert라면 1byte 더해주고 시작(data_region_off가 247로 저장되어 있기 때문)
	if(hdr.get_num_data() == 0){
		record_offset++;
	}

	uint64_t page_record_offset = (uint64_t)this + (uint64_t)record_offset;
	//3. hd offset 추가 및 정렬
	//3-1. scan -> 추가할 위치 찾음
	void *offset_arr = hdr.get_offset_array();

	int idx = hdr.get_num_data();
	for(int i = 0; i < hdr.get_num_data(); i++){
		uint16_t offsetarr_off = *(uint16_t *)((uint64_t)offset_arr+i*2);
		void *data_off = (void *)((uint64_t)this + (uint64_t)offsetarr_off);
		char *data_key = get_key(data_off);
		if (strcmp(key, data_key) < 0)
		{
			idx = i; //삽입할 위치
			break;
		}
	}

	//3-2. idx 이후의 놈들 다 오른쪽으로 한칸씩 이동
	for (int i = hdr.get_num_data(); i > idx; --i) {
            uint16_t next_off = *(uint16_t *)((uint64_t)offset_arr + (i-1)*2);
			void *cur_off = (void *)((uint64_t)offset_arr + i*2);
            put2byte(cur_off, next_off);
    }

	//4. 새로운 record 삽입 및 header update
	//4-1. (data) record update
	//a) record size 저장하기
	put2byte((void *)(page_record_offset), record_size);
	//b) key 저장하기
	memcpy((void *)((page_record_offset + sizeof(uint16_t))), key, key_len + 1);
	//c) value 저장하기
	uint64_t *value_off = ((uint64_t *)(page_record_offset + sizeof(uint16_t) + key_len + 1));
	*value_off = val;

	//4-2. (hd) offset_arr : idx 위치에 현재 거 삽입
	put2byte((void *)((uint64_t)offset_arr + idx*2), record_offset);

	//4-3. (hd) data_region_off update
	hdr.set_data_region_off(record_offset);
	//4-4. (hd) num_data update
	hdr.set_num_data(hdr.get_num_data() + 1);

	return true;
}

page* page::split(char *key, uint64_t val, char** parent_key){
	// Please implement this function in project 3.
	page *new_page;
	return new_page;
}

bool page::is_full(uint64_t inserted_record_size){
	// Please implement this function in project 2.
	return ((hdr.get_data_region_off() - sizeof(slot_header) - inserted_record_size) > PAGE_SIZE);
}

void page::defrag(){
	page *new_page = new page(get_type());
	int num_data = hdr.get_num_data();
	void *offset_array=hdr.get_offset_array();
	void *stored_key=nullptr;
	uint16_t off=0;
	uint64_t stored_val=0;
	void *data_region=nullptr;

	for(int i=0; i<num_data/2; i++){
		off= *(uint16_t *)((uint64_t)offset_array+i*2);	
		data_region = (void *)((uint64_t)this+(uint64_t)off);
		stored_key = get_key(data_region);
		stored_val= get_val((void *)stored_key);
		new_page->insert((char*)stored_key,stored_val);
	}	
	new_page->set_leftmost_ptr(get_leftmost_ptr());

	memcpy(this, new_page, sizeof(page));
	hdr.set_offset_array((void*)((uint64_t)this+sizeof(slot_header)));
	delete new_page;

}

void page::print(){
	uint32_t num_data = hdr.get_num_data();
	uint16_t off=0;
	uint16_t record_size= 0;
	void *offset_array=hdr.get_offset_array();
	void *stored_key=nullptr;
	uint64_t stored_val=0;

	printf("## slot header\n");
	printf("Number of data :%d\n",num_data);
	printf("offset_array : |");
	for(int i=0; i<num_data; i++){
		off= *(uint16_t *)((uint64_t)offset_array+i*2);	
		printf(" %d |",off);
	}
	printf("\n");

	void *data_region=nullptr;
	for(int i=0; i<num_data; i++){
		off= *(uint16_t *)((uint64_t)offset_array+i*2);	
		data_region = (void *)((uint64_t)this+(uint64_t)off);
		record_size = get_record_size(data_region);
		stored_key = get_key(data_region);
		stored_val= get_val((void *)stored_key);
		printf("==========================================================\n");
		printf("| data_sz:%u | key: %s | val :%lu | key_len:%lu\n",record_size,(char*)stored_key, stored_val,strlen((char*)stored_key));
	}
}