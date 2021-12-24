#ifndef INCLUDED_AVXBMP__HELPERS_H
#define INCLUDED_AVXBMP__HELPERS_H

//  std headers
#include<cfloat>
#include<cstdint>
#include<limits>
#include<stdexcept>
#include<string>
#include<type_traits>

namespace avxbmp::helpers {

    template <typename TYPE> constexpr inline TYPE inline_int_null_value();
    template <typename TYPE> constexpr inline TYPE inline_fp_null_value();
    template <typename TYPE> constexpr inline TYPE null_builder();
    template <typename TYPE> std::string get_type_name();

}  // namespace avxbmp::helpers


// Inline functions implementations //
template <typename TYPE> 
constexpr inline TYPE 
avxbmp::helpers::inline_int_null_value() {
  return std::is_signed<TYPE>::value 
            ? std::numeric_limits<TYPE>::min()
            : std::numeric_limits<TYPE>::max();
}

template <> 
constexpr inline float avxbmp::helpers::inline_fp_null_value<float>() {
  return FLT_MIN;
}

template <> 
constexpr inline double avxbmp::helpers::inline_fp_null_value<double>() {
  return DBL_MIN;
}

template <typename TYPE> 
constexpr TYPE avxbmp::helpers::null_builder() 
{
  static_assert(std::is_floating_point_v<TYPE> || std::is_integral_v<TYPE>,
                "Unsupported type");

  if constexpr (std::is_floating_point_v<TYPE>) {
    return inline_fp_null_value<TYPE>();
  } else if constexpr (std::is_integral_v<TYPE>) {
    return inline_int_null_value<TYPE>();
  }
}

template<typename TYPE>
std::string  avxbmp::helpers::get_type_name() {
    if constexpr (std::is_same<TYPE,  char>::value)  return "char";
    else if constexpr (std::is_same<TYPE,  int8_t>::value)  return "int8_t";
    else if constexpr (std::is_same<TYPE,  uint8_t>::value) return "uint8_t";
    else if constexpr (std::is_same<TYPE,  int32_t>::value) return "int32_t";
    else if constexpr (std::is_same<TYPE, uint32_t>::value) return "uint32_t";
    else if constexpr (std::is_same<TYPE,  int64_t>::value) return "int64_t";
    else if constexpr (std::is_same<TYPE, uint64_t>::value) return "uint64_t";
    else if constexpr (std::is_same<TYPE,  double>::value) return "double";
    else if constexpr (std::is_same<TYPE,  float>::value)  return "float";
    else {
        throw std::runtime_error ("get_type_name() -- unsupported type");
    }
}

#endif // INCLUDED_AVXBMP__HELPERS_H