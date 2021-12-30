#include <immintrin.h>
#include <cstddef>
#include <cstdint>

#include <bitset>
#include <iostream>

template <typename MM512_TYPE>
static void print_m512(const MM512_TYPE& val) {
  const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&val);
  for (size_t i = 0; i < 64; ++i) {
    std::cout << (bytes[i] == 0 ? "0" : "X");
  }
  std::cout << std::endl;
}

// cf: 
//   https://www.officedaytime.com/simd512e/simdimg/pcmpv5.php?f=vpcmpneqb
//   https://www.officedaytime.com/simd512e/simdimg/pcmpv5.php?f=vpcmpneqd
extern "C" __attribute__((target("avx512bw","avx512f"), optimize("no-tree-vectorize"))) size_t
gen_bitmap_avx512_8_intr(uint8_t* dst,
                         size_t* null_count_out,
                         uint8_t* src,
                         size_t size,
                         uint64_t null_val) {
  // rdi: *dst
  // rsi: *null_count
  // rdx: *src
  // rcx: size
  // r8:  nullvalues (byte-repeated) for the type

  // NOTE: use these 2 lines if only "avx512bw" is required 
  // __m128d tmp = _mm_load_pd(reinterpret_cast<double*>(&null_val));
  // __m512i nulls_mask = _mm512_broadcastq_epi64(
  //     *reinterpret_cast<__m128i*>(&tmp));  // vpbroadcastq  %r8, %zmm0
  __m512i nulls_mask = _mm512_set1_epi64 ((int64_t)null_val); 

  size_t null_count = 0;
  while (size > 0) {
    __m512d loaded_bytes =
        _mm512_loadu_pd(static_cast<void*>(src));  //  vmovupd     0(%rdx), %zmm10

    // comparing nulls stored in loaded_bytes and nulls_mask
    __mmask64 k1 = _mm512_cmpneq_epi8_mask(*reinterpret_cast<__m512i*>(&loaded_bytes),
                                           nulls_mask);  // vpcmpneqb   %zmm0, %zmm10, %k1

    // saving 64-bit bitmap to *dst
    *reinterpret_cast<__mmask64*>(dst) = k1;  // movq        %rax, 0(%rdi)
    // counting nulls, storing nulls count to %rax
    null_count += _mm_popcnt_u64(~k1);  // not         %rax
                                        // popcnt	    %rax, %rax
                                        // addq        %rax, %r9

    // advancing *dst by 8 bytes, *src by 64 bytes
    dst += 8;   // addq        $8,  %rdi
    src += 64;  // addq        $64, %rdx

    // 64 bytes processed, decrementing count of remaining bytes by 64
    size -= 64;  // subq        $64, %rcx
  }

  *null_count_out = null_count;
  return null_count;
}



extern "C" __attribute__((target("avx512bw","avx512f"), optimize("no-tree-vectorize"))) size_t
gen_bitmap_avx512_32_intr(uint8_t* dst,
                         size_t* null_count_out,
                         uint8_t* src,
                         size_t size,
                         uint64_t null_val) {
  // rdi: *dst
  // rsi: *null_count
  // rdx: *src
  // rcx: size
  // r8:  nullvalues (byte-repeated) for the type

  __m512i nulls_mask = _mm512_set1_epi64 ((int64_t)null_val); 
  // NOTE: use these 2 lines if only "avx512bw" is required 
  // __m128d tmp = _mm_load_pd(reinterpret_cast<double*>(&null_val));
  // __m512i nulls_mask = _mm512_broadcastq_epi64(
  //     *reinterpret_cast<__m128i*>(&tmp));  // vpbroadcastq  %r8, %zmm0


  size_t null_count = 0;
  while (size > 0) {
    __m512d loaded_bytes =
        _mm512_loadu_pd(static_cast<void*>(src));  //  vmovupd     0(%rdx), %zmm10

    // comparing nulls stored in loaded_bytes and nulls_mask
    __mmask16  k1 = _mm512_cmpneq_epi32_mask(*reinterpret_cast<__m512i*>(&loaded_bytes),
                                             nulls_mask);  // vpcmpneqd   %zmm0, %zmm10, %k1

  
    // saving 16-bit bitmap to *dst (64 bytes of input/4 byte-long double word yield 16 comparisons, hence 16 bits)
    *reinterpret_cast<__mmask16*>(dst) = k1;  // mov         %ax, 0(%rdi) : TODO CHANGE ACCORDINGLY
  
  
    // counting nulls, storing nulls count to %rax
    null_count += _mm_popcnt_u32((~k1 )& 0xFFFF);  
                                        // not         %ax
                                        // popcntl	    %eax, %eax
                                        // addq        %rax, %r9

       
    // advancing *dst by 2 bytes, *src by 64 bytes
    dst += 2;   // addq        $2,  %rdi
    src += 64;  // addq        $64, %rdx

    // 64 bytes processed, decrementing count of remaining [u]int32_t by 16=64/4
    size -= 16;  // subq        $64, %rcx
  }

  *null_count_out = null_count;
  return null_count;
}

extern "C" __attribute__((target("avx512bw","avx512f"), optimize("no-tree-vectorize"))) size_t
gen_bitmap_avx512_64_intr(uint8_t* dst,
                         size_t* null_count_out,
                         uint8_t* src,
                         size_t size,
                         uint64_t null_val) {
  // rdi: *dst
  // rsi: *null_count
  // rdx: *src
  // rcx: size
  // r8:  nullvalues (byte-repeated) for the type

  __m512i nulls_mask = _mm512_set1_epi64 ((int64_t)null_val); 

  // NOTE: use these 2 lines if only "avx512bw" is required 
  // __m128d tmp = _mm_load_pd(reinterpret_cast<double*>(&null_val));
  // __m512i nulls_mask = _mm512_broadcastq_epi64(
  //     *reinterpret_cast<__m128i*>(&tmp));  // vpbroadcastq  %r8, %zmm0

  size_t null_count = 0;
  while (size > 0) {
    __m512d loaded_bytes =
        _mm512_loadu_pd(static_cast<void*>(src));  //  vmovupd     0(%rdx), %zmm10

    // comparing nulls stored in loaded_bytes and nulls_mask
    __mmask8 k1 = _mm512_cmpneq_epi64_mask(*reinterpret_cast<__m512i*>(&loaded_bytes),
                                           nulls_mask);  // vpcmpneqq   %zmm0, %zmm10, %k1

    // saving 8-bit bitmap to *dst (64 bytes of input/8 byte-long quad word yield 8 comparisons, hence 8 bits)
    *reinterpret_cast<__mmask8*>(dst) = k1;  //     mov         %al, 0(%rdi)

    // counting nulls, storing nulls count to %rax
    null_count += _mm_popcnt_u32((~k1 )& 0xFF);  
                                        // not         %al
                                        // popcntl	   %eax, %eax
                                        // addq        %rax, %r9

    // advancing *dst by 1 byte, *src by 64 bytes
    ++dst;      // inc         %rdi
    src += 64;  // addq        $64, %rdx
    // 8 qwords processed, decrementing count of remaining qwords by 8
    size -= 8;  // subq        $8, %rcx
  }

  *null_count_out = null_count;
  return null_count;
}