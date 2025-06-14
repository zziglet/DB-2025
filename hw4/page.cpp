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
	hdr.set_num_data(0);
	hdr.set_data_region_off(PAGE_SIZE-1-sizeof(page*));
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

uint64_t page::find(char *key) {
	// Please implement this function in project 2.
	
	// offset arr의 시작 지점부터 체크
	void *offset_array = hdr.get_offset_array();
	int num_data = hdr.get_num_data();

	for (int i = 0; i < num_data; i++) {
			uint16_t off = *(uint16_t *)((uint64_t)offset_array + i * 2);
			void *record_ptr = (void *)((uint64_t)this + off);
			char *stored_key = get_key(record_ptr);
			
			//인자로 받은 key와 일치하는 것이 있는지 체크
			if (strcmp(stored_key, key) == 0) {
					return get_val(stored_key);
			}
	}
	return 0;
}

bool page::insert(char *key, uint64_t val) {
	// Please implement this function in project 2.

	// 1. record_size 계산 : record size(2byte = 16bit) + key 길이(null 포함)+ value(8byte = 64bit)
	uint16_t key_len = strlen(key) + 1;
	uint16_t record_size = sizeof(uint16_t) + key_len + sizeof(uint64_t);

	// 2. space 여유 공간 체크 => 공간 없으면 insert 종료(exit point)
	if (is_full(record_size)) return false;

	// 3. insert offset 계산 후 pointer 설정
	uint16_t insert_off = hdr.get_data_region_off() - record_size + 1;
	void *record_ptr = (void *)((uint64_t)this + insert_off); 

	// 4. 레코드 크기 저장, key/value 저장
	put2byte(record_ptr, record_size);
	memcpy((char *)record_ptr + sizeof(uint16_t), key, key_len);
	memcpy((char *)record_ptr + sizeof(uint16_t) + key_len, &val, sizeof(uint64_t));

	// 5. offset_array 오름차순 정렬
	void *offset_array = hdr.get_offset_array();
	int num_data = hdr.get_num_data();
	int insert_idx = 0;
	for (; insert_idx < num_data; insert_idx++) {
			uint16_t off = *(uint16_t *)((uint64_t)offset_array + insert_idx * 2);
			void *r = (void *)((uint64_t)this + off);
			if (strcmp(key, get_key(r)) < 0) break;
	}

	// 6. 새로 삽입하기 위해 저장할 공간 밀어서 만들고 삽입
	memmove((uint8_t *)offset_array + (insert_idx + 1) * 2,
					(uint8_t *)offset_array + insert_idx * 2,
					(num_data - insert_idx) * 2); //메모리 영역 겹쳐도 안전하게 이동
	put2byte((uint8_t *)offset_array + insert_idx * 2, insert_off);

	// 7. slot header 업데이트
	hdr.set_num_data(num_data + 1);
	hdr.set_data_region_off(insert_off);

	return true;
}

page* page::split(char *key, uint64_t val, char** parent_key){
	// Please implement this function in project 3.
	page *new_page;
	return new_page;
}

bool page::is_full(uint64_t inserted_record_size) {
	// Please implement this function in project 2.

	uint16_t data_off = hdr.get_data_region_off();
	uint16_t offset_array_start = sizeof(slot_header);
	uint16_t offset_array_used = hdr.get_num_data() * sizeof(uint16_t);

	uint16_t available_space = data_off - (offset_array_start + offset_array_used) + 1;
	uint16_t required_space = inserted_record_size + sizeof(uint16_t);

	return available_space < required_space;
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