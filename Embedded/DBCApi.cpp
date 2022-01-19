
#include "DBEngine.h"

#include <iostream>

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
}
