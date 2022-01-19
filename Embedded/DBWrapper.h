

extern "C" {

void* create_db_engine(const char* cmd_line);

void destroy_db_engine(void* db_engine);

void execute_ddl(const char* query);

void execute_dml(const char* query);
}
