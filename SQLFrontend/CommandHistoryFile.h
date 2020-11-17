/*
 * Copyright 2019 OmniSci, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef COMMANDHISTORYFILE_H
#define COMMANDHISTORYFILE_H

#include <iostream>
#include <string>
#include <utility>

#include <sys/types.h>
#ifdef _WIN32
#include <folly/portability/unistd.h>
using namespace folly::portability::unistd;
#else
#include <pwd.h>
#include <unistd.h>
#endif

inline constexpr char const* const getDefaultHistoryFilename() {
  return ".omnisql_history";
}

class DefaultEnvResolver {
 public:
  auto getuid() const { return ::getuid(); }
#ifndef _WIN32
  auto const* getpwuid(decltype(::getuid()) uid) const { return ::getpwuid(uid); }
#endif

#if defined(_APPLE__) || defined(_WIN32)
  auto const* getenv(char const* env_var_name) const {return ::getenv(env_var_name); }
#else
  auto const* getenv(char const* env_var_name) const {
    return ::secure_getenv(env_var_name);
  }
#endif
};

template <typename ENV_RESOLVER>
class CommandHistoryFileImpl : private ENV_RESOLVER {
 public:
  CommandHistoryFileImpl() : CommandHistoryFileImpl(resolveCommandFile()) {}
  explicit CommandHistoryFileImpl(std::string command_file_name)
      : command_file_name_(command_file_name) {}

  operator char const*() const { return command_file_name_.c_str(); }

  template <typename ER>
  friend inline std::istream& operator>>(std::istream& i,
                                         CommandHistoryFileImpl<ER>& cmd_file);
  template <typename ER>
  friend inline std::ostream& operator<<(std::ostream& o,
                                         CommandHistoryFileImpl<ER> const& cmd_file);

 private:
  auto resolveHomeDirectory() const {
    auto* home_env = this->getenv("HOME");
    if (home_env == nullptr) {
#ifdef _WIN32
#ifdef _UNICODE
      wchar_t home_dir_w[MAX_PATH];
      if (SUCCEEDED(SHGetFolderPath(NULL, CSIDL_PERSONAL, NULL, 0, home_dir_w))) {
        wcstombs(home_dir_, home_dir_w, MAX_PATH);
        home_env = home_dir_;
      }
#else
      if (SUCCEEDED(SHGetFolderPath(NULL, CSIDL_PERSONAL, NULL, 0, home_dir_))) {
        home_env = home_dir_;
      }
#endif
#else
      auto* passwd_entry = ENV_RESOLVER::getpwuid(ENV_RESOLVER::getuid());
      if (passwd_entry != nullptr) {
        home_env = passwd_entry->pw_dir;
      }
#endif
    }
    return home_env;
  }

  std::string const resolveCommandFile() const {
    auto* home_dir = resolveHomeDirectory();
    if (home_dir == nullptr) {  // Just use default command history file name in current
                                // dir in this scenario
      return std::string(getDefaultHistoryFilename());
    }
    return std::string(home_dir) + '/' + getDefaultHistoryFilename();
  }

 private:
#ifdef _WIN32
  mutable char home_dir_[MAX_PATH];
#endif
  std::string command_file_name_;
};

template <typename ENV_RESOLVER>
inline std::ostream& operator<<(std::ostream& o,
                                CommandHistoryFileImpl<ENV_RESOLVER> const& cmd_file) {
  o << cmd_file.command_file_name_;
  return o;
}

template <typename ENV_RESOLVER>
inline std::istream& operator>>(std::istream& i,
                                CommandHistoryFileImpl<ENV_RESOLVER>& cmd_file) {
  i >> cmd_file.command_file_name_;
  return i;
}

using CommandHistoryFile = CommandHistoryFileImpl<DefaultEnvResolver>;

#endif
