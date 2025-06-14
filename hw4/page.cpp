#include "page.hpp"
#include <iostream>
#include <cstring>
#include <atomic>

void put2byte(void *dest, uint16_t data)
{
	*(uint16_t *)dest = data;
}

uint16_t get2byte(void *dest)
{
	return *(uint16_t *)dest;
}

page::page(uint16_t type)
{
	hdr.set_num_data(0);
	hdr.set_data_region_off(PAGE_SIZE - 1 - sizeof(page *));
	hdr.set_offset_array((void *)((uint64_t)this + sizeof(slot_header)));
	hdr.set_page_type(type);
}

uint16_t page::get_type()
{
	return hdr.get_page_type();
}

uint16_t page::get_record_size(void *record)
{
	uint16_t size = *(uint16_t *)record;
	return size;
}

char *page::get_key(void *record)
{
	char *key = (char *)((uint64_t)record + sizeof(uint16_t));
	return key;
}

uint64_t page::get_val(void *key)
{
	uint64_t val = *(uint64_t *)((uint64_t)key + (uint64_t)strlen((char *)key) + 1);
	return val;
}

void page::set_leftmost_ptr(page *p)
{
	leftmost_ptr = p;
}

page *page::get_leftmost_ptr()
{
	return leftmost_ptr;
}

uint64_t page::find(char *key)
{
	// Please implement this function in project 2.

	// 1. offset arr의 시작 지점부터 체크
	void *offset_array = hdr.get_offset_array();
	int num_data = hdr.get_num_data();

	// 2-1. Leaf page(기존 코드)
	if (get_type() == LEAF)
	{
		for (int i = 0; i < num_data; i++)
		{
			uint16_t off = *(uint16_t *)((uint64_t)offset_array + i * 2);
			void *record_ptr = (void *)((uint64_t)this + off);
			char *stored_key = get_key(record_ptr);

			// 2-1-1. 인자로 받은 key와 일치하는 것이 있는지 체크
			if (strcmp(stored_key, key) == 0)
			{
				return get_val(stored_key);
			}
		}

		// 2-1-2. 존재하지 않음
		return 0;
	}

	// 2-2. Internal page(내부 페이지)
	for (int i = 0; i < num_data; i++)
	{
		uint16_t off = *(uint16_t *)((uint64_t)offset_array + i * 2);
		void *record_ptr = (void *)((uint64_t)this + off);
		char *stored_key = get_key(record_ptr);

		// 인자로 받은 key와 비교
		if (strcmp(key, stored_key) < 0)
		{
			// 2-2-1. key가 더 작다면, leftmost 자식노드로 이동
			if (i == 0)
			{
				return (uint64_t)get_leftmost_ptr();
			}
			else
			{
				// 2-2-2. key가 더 크다면
				uint16_t prev_off = *(uint16_t *)((uint64_t)offset_array + (i - 1) * 2);
				void *prev_record_ptr = (void *)((uint64_t)this + prev_off);
				char *prev_key = get_key(prev_record_ptr);
				return get_val(prev_key);
			}
		}
	}

	// rightmost 자식노드로 이동
	if (num_data > 0)
	{
		uint16_t last_off = *(uint16_t *)((uint64_t)offset_array + (num_data - 1) * 2);
		void *last_record_ptr = (void *)((uint64_t)this + last_off);
		char *last_key = get_key(last_record_ptr);
		return get_val(last_key);
	}

	// 2-2-2. 존재하지 않음
	return 0;
}

bool page::insert(char *key, uint64_t val)
{
	// Please implement this function in project 2.

	// 1. record_size 계산 : [record_size (2 bytes)][key (null-terminated)][val (8 bytes)]
	uint16_t key_len = strlen(key) + 1;
	uint16_t record_size = sizeof(uint16_t) + key_len + sizeof(uint64_t);

	// 2. space 여유 공간 체크 => 공간 없으면 insert 종료(exit point)
	if (is_full(record_size))
		return false;

	// 3. insert offset 계산 후 pointer 설정
	uint16_t insert_off = hdr.get_data_region_off() - record_size + 1;
	void *record_ptr = (void *)((uint64_t)this + insert_off);

	// 4. 레코드 크기 저장, key/value 저장
	// 새로운 저장 방식: [record_size (2 bytes)][key (null-terminated)][val (8 bytes)]
	memcpy(record_ptr, &record_size, sizeof(uint16_t));									 // record_size
	memcpy((uint8_t *)record_ptr + 2, key, key_len);										 // key
	memcpy((uint8_t *)record_ptr + 2 + key_len, &val, sizeof(uint64_t)); // val

	// 5. offset_array 오름차순 정렬
	void *offset_array = hdr.get_offset_array();
	int num_data = hdr.get_num_data();
	int insert_idx = 0;
	for (; insert_idx < num_data; insert_idx++)
	{
		uint16_t off = *(uint16_t *)((uint64_t)offset_array + insert_idx * 2);
		void *r = (void *)((uint64_t)this + off);
		if (strcmp(key, get_key(r)) < 0)
			break;
	}

	// 6. 새로 삽입하기 위해 저장할 공간 밀어서 만들고 삽입
	memmove((uint8_t *)offset_array + (insert_idx + 1) * 2,
					(uint8_t *)offset_array + insert_idx * 2,
					(num_data - insert_idx) * 2); // 메모리 영역 겹쳐도 안전하게 이동
	put2byte((uint8_t *)offset_array + insert_idx * 2, insert_off);

	// 7. slot header 업데이트
	hdr.set_num_data(num_data + 1);
	hdr.set_data_region_off(insert_off);
	if (this->get_type() != LEAF)
	{
		__atomic_fetch_add(&version, 2, __ATOMIC_RELEASE); // 짝수 유지
	}

	return true;
}

page *page::split(char *key, uint64_t val, char **parent_key)
{
	// Please implement this function in project 3.

	// 1. 현재 페이지와 동일한 타입의 새 페이지 생성
	page *new_page = new page(this->get_type());

	uint16_t num = hdr.get_num_data();				 // 현재 페이지의 레코드 수
	uint32_t mid = (num + 1) / 2;							 // 절반 기준 인덱스 계산
	void *offset_arr = hdr.get_offset_array(); // offset array 포인터

	// 2. internal node : 중앙값을 상위로 올리므로 해당 키는 복사하지 않는다
	if (new_page->get_type() != LEAF)
	{
		mid--;
	}

	// 3. mid 이후의 데이터를 새 페이지로 복사
	for (int i = mid; i < num; ++i)
	{
		uint16_t offset = *(uint16_t *)((uint64_t)offset_arr + i * 2);
		void *record_ptr = (void *)((uint64_t)this + offset);
		char *k = (char *)(get_key(record_ptr));
		uint64_t v = get_val((void *)(get_key(record_ptr)));
		new_page->insert(k, v);
	}

	// 4. 상위 노드로 올릴 parent_key 설정 (new_page의 첫 번째 key 사용)
	void *new_offset_arr = new_page->hdr.get_offset_array();
	uint16_t new_off = *(uint16_t *)((uint64_t)new_offset_arr);
	void *new_record = (void *)((uint64_t)new_page + new_off);
	char *new_split_k = get_key(new_record);
	*parent_key = new char[strlen(new_split_k) + 1];
	strcpy(*parent_key, new_split_k);

	// version을 안정적인 짝수 상태로 설정
	uint64_t prev_version = read_version();
	// defrag 수행 (version 초기화됨)
	defrag();
	// write 잠금이 끝난 상태로 복원하려면 짝수 version 필요
	if (prev_version % 2 == 1)
		prev_version++; // 홀수였다면 다음 짝수로
	__atomic_store_n(&version, prev_version, __ATOMIC_RELEASE);
	if (this->get_type() != LEAF)
	{
		__atomic_fetch_add(&version, 2, __ATOMIC_RELEASE);
	}
	// 5. 새로운 key를 적절한 페이지에 삽입
	bool inserted = false;
	// 수정된 비교 방식
	char *cmp_key = get_key((void *)((uint64_t)this + *(uint16_t *)((uint64_t)offset_arr + mid * 2)));
	if (strcmp(key, cmp_key) < 0)
	{
		inserted = insert(key, val);
	}
	else
	{
		inserted = new_page->insert(key, val);
	}

	// 만약 삽입이 실패했다면, 새 페이지를 삭제하고 nullptr 반환
	if (!inserted)
	{
		std::cerr << "[SPLIT] Failed to insert key " << key << " after split\n";
		delete new_page;
		return nullptr;
	}

	// 6. internal node : leftmost_ptr를 적절히 설정
	if (this->get_type() != LEAF)
	{
		new_page->set_leftmost_ptr(get_leftmost_ptr());  // new_page inherits original leftmost child
		// get_val returns the right pointer associated with parent_key
		set_leftmost_ptr(reinterpret_cast<page *>(get_val((void *)*parent_key)));  // this now points to correct mid split
	}

	this->write_unlock();
	return new_page;
}

bool page::is_full(uint64_t inserted_record_size)
{
	// Please implement this function in project 2.

	uint16_t data_off = hdr.get_data_region_off();
	uint16_t offset_array_start = sizeof(slot_header);
	uint16_t offset_array_used = hdr.get_num_data() * sizeof(uint16_t);
	uint16_t available_space = data_off - (offset_array_start + offset_array_used) + 1;
	uint16_t required_space = inserted_record_size + sizeof(uint16_t);
	return available_space < required_space;
}

void page::defrag()
{
	page *new_page = new page(get_type());
	int num_data = hdr.get_num_data();
	void *offset_array = hdr.get_offset_array();
	void *stored_key = nullptr;
	uint16_t off = 0;
	uint64_t stored_val = 0;
	void *data_region = nullptr;

	for (int i = 0; i < num_data; i++)
	{
		off = *(uint16_t *)((uint64_t)offset_array + i * 2);
		data_region = (void *)((uint64_t)this + (uint64_t)off);
		stored_key = get_key(data_region);
		stored_val = get_val((void *)stored_key);
		new_page->insert((char *)stored_key, stored_val);
	}
	new_page->set_leftmost_ptr(get_leftmost_ptr());

	memcpy(this, new_page, sizeof(page));
	hdr.set_offset_array((void *)((uint64_t)this + sizeof(slot_header)));
	delete new_page;
}

void page::print()
{
	uint32_t num_data = hdr.get_num_data();
	uint16_t off = 0;
	uint16_t record_size = 0;
	void *offset_array = hdr.get_offset_array();
	void *stored_key = nullptr;
	uint64_t stored_val = 0;

	printf("## slot header\n");
	printf("Number of data :%d\n", num_data);
	printf("offset_array : |");
	for (int i = 0; i < num_data; i++)
	{
		off = *(uint16_t *)((uint64_t)offset_array + i * 2);
		printf(" %d |", off);
	}
	printf("\n");

	void *data_region = nullptr;
	for (int i = 0; i < num_data; i++)
	{
		off = *(uint16_t *)((uint64_t)offset_array + i * 2);
		data_region = (void *)((uint64_t)this + (uint64_t)off);
		record_size = get_record_size(data_region);
		stored_key = get_key(data_region);
		stored_val = get_val((void *)stored_key);
		printf("==========================================================\n");
		printf("| data_sz:%u | key: %s | val :%lu | key_len:%lu\n", record_size, (char *)stored_key, stored_val, strlen((char *)stored_key));
	}
}

// Version-based Locking Implementation
uint64_t page::read_version()
{
	// 현재 version을 읽어 반환
	return __atomic_load_n(&version, __ATOMIC_ACQUIRE);
}

bool page::validate_read(uint64_t old_version)
{
	// 현재 version을 읽어 old_version과 비교
	uint64_t current_version = __atomic_load_n(&version, __ATOMIC_ACQUIRE);
	return current_version == old_version;
}

bool page::try_read_lock(uint64_t version)
{
	// 파라미터로 받은 version이 현재 접근 가능한지 확인
	// 사용 가능이란 뜻 = 짝수
	return (version % 2 == 0);
}

bool page::try_write_lock()
{
	// 페이지가 사용 가능한 상태인지 확인
	uint64_t expected = __atomic_load_n(&version, __ATOMIC_ACQUIRE);

	// 짝수면 사용 가능
	while (expected % 2 == 0)
	{
		uint64_t desired = expected + 1; // 홀수로 변경
		if (__atomic_compare_exchange_n(&version, &expected, desired,
																		false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
		{
			return true; // lock 얻기 성공
		}
		// lock 얻기 실패, expected는 현재 version으로 업데이트됨
	}
	// 현재 version이 홀수면 사용 불가능
	// 즉, 다른 쓰레드가 쓰기 작업을 하고 있는 중
	return false;
}

bool page::read_unlock(uint64_t old_version)
{
	// validate_read를 통해 버전이 바뀌었는지 화깅ㄴ
	return validate_read(old_version);
}

void page::write_unlock()
{
	// 쓰기 작업이 끝났으므로 version을 홀수로 변경
	__atomic_fetch_add(&version, 1, __ATOMIC_ACQ_REL);
}

// 추가: split 후 자식 페이지를 찾는 함수
uint64_t page::find_child(char *key)
{
	void *offset_array = hdr.get_offset_array();
	int num_data = hdr.get_num_data();

	for (int i = 0; i < num_data; i++)
	{
		uint16_t off = *(uint16_t *)((uint64_t)offset_array + i * 2);
		void *record_ptr = (void *)((uint64_t)this + off);
		char *stored_key = get_key(record_ptr);

		if (strcmp(key, stored_key) < 0)
		{
			if (i == 0)
			{
				return reinterpret_cast<uint64_t>(get_leftmost_ptr());
			}
			else
			{
				uint16_t prev_off = *(uint16_t *)((uint64_t)offset_array + (i - 1) * 2);
				void *prev_record_ptr = (void *)((uint64_t)this + prev_off);
				char *prev_key = get_key(prev_record_ptr);
				uint64_t child_ptr = get_val(prev_key);
				return child_ptr;
			}
		}
	}

	uint16_t last_off = *(uint16_t *)((uint64_t)offset_array + (num_data - 1) * 2);
	void *last_record_ptr = (void *)((uint64_t)this + last_off);
	char *last_key = get_key(last_record_ptr);
	uint64_t child_ptr = get_val(last_key);
	return child_ptr;
}