#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

using namespace duckdb;

namespace {

// A storage extension that rewrites the attach path - the shape of every remote backend that
// resolves an endpoint and credentials at attach time - and hands the prompt a readable stand-in
// for the path the user actually typed.
struct RewritingStorageExtension : StorageExtension {
	RewritingStorageExtension() {
		attach = [](optional_ptr<StorageExtensionInfo>, ClientContext &, AttachedDatabase &db, const string &,
		            AttachInfo &info, AttachOptions &options) -> unique_ptr<Catalog> {
			options.connect_display = "widget:" + info.path;
			info.path = IN_MEMORY_PATH;
			return make_uniq_base<Catalog, DuckCatalog>(db);
		};
		create_transaction_manager = [](optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
		                                Catalog &) -> unique_ptr<TransactionManager> {
			return make_uniq<DuckTransactionManager>(db);
		};
	}
};

Catalog &AttachedCatalog(Connection &con, const string &name) {
	auto attached = DatabaseManager::Get(*con.context).GetDatabase(*con.context, Identifier(name));
	REQUIRE(attached);
	return attached->GetCatalog();
}

} // namespace

TEST_CASE("Test storage extension connect display override", "[api]") {
	DBConfig config;
	StorageExtension::Register(config, "widget", make_shared_ptr<RewritingStorageExtension>());

	DuckDB db(nullptr, &config);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("ATTACH 'my-cluster' AS w (TYPE WIDGET)"));
	auto &catalog = AttachedCatalog(con, "w");

	// The override wins over whatever the catalog itself would display, so the rewritten path
	// never reaches the prompt.
	REQUIRE(catalog.GetConnectLabel() == "widget:my-cluster");
	REQUIRE(catalog.GetAttached().GetConnectDisplayOverride().has_value());
}

TEST_CASE("Test connect display without an override", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("ATTACH ':memory:' AS plain"));
	auto &catalog = AttachedCatalog(con, "plain");

	// No override set: GetConnectLabel is GetConnectDisplay, which falls back to the attach alias.
	REQUIRE(!catalog.GetAttached().GetConnectDisplayOverride().has_value());
	REQUIRE(catalog.GetConnectLabel() == catalog.GetConnectDisplay());
	REQUIRE(catalog.GetConnectLabel() == "plain");
}
