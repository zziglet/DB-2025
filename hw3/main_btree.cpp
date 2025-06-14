#include "btree.hpp"
#include <iostream> 
#define STRING_LEN 20

int main(){
	btree *tree = new btree();

	char key[STRING_LEN];
	char i;
	uint64_t val = 100;
	uint64_t cnt = 0;

	for(i='a'; i<='z'; i+=1){
		for(int j=0; j<STRING_LEN-1; j++){
			key[j] = i;
		}
		key[STRING_LEN-1]='\0';
		cnt++;
		val*=cnt;
		tree->insert(key, val);
	}

	val = 100;
	cnt = 0;
	for(i='a'; i<='z'; i+=1){
		for(int j=0; j<STRING_LEN-1; j++){
			key[j] = i;
		}
		key[STRING_LEN-1]='\0';
		cnt++;
		val*=cnt;
		if(val== tree->lookup(key)){
			printf("key :%s founds\n",key);		
		}
		else{
			printf("key :%s Something wrong\n",key);		
		}

	}
	// 랜덤 문자열 키 삽입 및 조회 테스트
	std::vector<std::string> random_keys = {
		"dog", "cat", "zebra", "apple", "mango",
		"grape", "peach", "lemon", "kiwi", "melon"
	};

	std::cout << "\n[Random Insert Test]\n";
	for (int i = 0; i < random_keys.size(); ++i) {
		tree->insert(const_cast<char*>(random_keys[i].c_str()), i + 1000);
	}

	std::cout << "[Random Lookup Test]\n";
	for (int i = 0; i < random_keys.size(); ++i) {
		uint64_t val = tree->lookup(const_cast<char*>(random_keys[i].c_str()));
		if (val == i + 1000) {
			std::cout << "key :" << random_keys[i] << " OK\n";
		} else {
			std::cout << "key :" << random_keys[i] << " FAIL (got " << val << ")\n";
		}
	}

	// 3. Split Cascade Test
	std::cout << "\n[Split Cascade Test]\n";
	for (int i = 0; i < 100; ++i) {
		char keybuf[20];
		sprintf(keybuf, "key%03d", i); // key000, key001, ...
		tree->insert(keybuf, i * 10);
	}
	for (int i = 0; i < 100; ++i) {
		char keybuf[20];
		sprintf(keybuf, "key%03d", i);
		uint64_t v = tree->lookup(keybuf);
		if (v != i * 10) {
			std::cout << "FAIL: " << keybuf << " got " << v << "\n";
		}
	}
	std::cout << "Split cascade insert/lookup OK if no FAIL above\n";
	return 0;

}
