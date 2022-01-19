
#include "DBEngine.h"

#include <iostream>

using namespace EmbeddedDatabase;

extern "C" {

static std::shared_ptr<EmbeddedDatabase::DBEngine> g_db_engine;

void* create_db_engine(const char* cmd_line) {
  try {
    if (!g_db_engine) {
      g_db_engine = EmbeddedDatabase::DBEngine::create(cmd_line);
    } else {
      std::cerr << "DBEngine already exists, returning existing engine." << std::endl;
    }

    return g_db_engine.get();
  } catch (const std::exception& e) {
    std::cerr << "Exception creating DBEngine: " << e.what() << std::endl;
  }
  return nullptr;
}

void destroy_db_engine(void* db_engine) {
  if (g_db_engine.get() != db_engine) {
    std::cerr << "DBEngine pointer does not match stored instance!" << std::endl;
  }
  g_db_engine.reset();
}

void execute_ddl(const char* query) {
  if (!g_db_engine) {
    std::cerr << "DBEngine does not exist!" << std::endl;
    return;
  }

  g_db_engine->executeDDL(std::string(query));
}

// TODO: deleter API, though this will delete everything on shutdown
static std::unordered_map<Cursor*, std::shared_ptr<Cursor>> g_cursors;

Cursor* execute_dml(const char* query) {
  if (!g_db_engine) {
    std::cerr << "DBEngine does not exist!" << std::endl;
    return nullptr;
  }

  auto cursor = g_db_engine->executeDML(std::string(query));
  CHECK(g_cursors.insert(std::make_pair(cursor.get(), cursor)).second);
  return cursor.get();
}
}
