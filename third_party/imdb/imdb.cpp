#include "include/imdb.hpp"
#include "imdb_constants.hpp"
#include "duckdb/common/file_system.hpp"
#include <iostream>

using namespace duckdb;
using namespace std;

namespace imdb {

void print_file(const string &file_name, bool _exists) {
	std::cout << file_name << std::endl;
}

void dbgen(DuckDB &db) {
	Connection con(db);
	con.Query("BEGIN TRANSACTION");
	auto file_system = FileSystem::CreateLocal();
	file_system->SetWorkingDirectory("third_party/imdb/data/");
	for (int t = 0; t < IMDB_TABLE_COUNT; t++) {
		con.Query(IMDB_TABLE_DDL[t]);
		string table_name = string(IMDB_TABLE_NAMES[t]);
		string data_file_name = table_name+".csv";
		if (!file_system->FileExists(data_file_name)) {
			throw Exception("IMDB data file missing, could not find " + data_file_name + ". try `make imdb` to download.");
		}
		con.Query("COPY "+table_name+" FROM '"+data_file_name+"' DELIMITER ',' ESCAPE '\\';");
		std::cout << "COPY "+table_name+" FROM '"+data_file_name+"' DELIMITER ',' ESCAPE '\\';" << std::endl;
	}
	con.Query("COMMIT");
}

string get_query(int query) {
	if (query <= 0 || query > IMDB_QUERIES_COUNT) {
		throw SyntaxException("Out of range IMDB query number %d", query);
	}
	return IMDB_QUERIES[query - 1];
}
} // namespace imdb
