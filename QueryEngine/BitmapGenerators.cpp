#include "BitmapGenerators.h"
#include <immintrin.h>

#define USE_AVX512_INTRINSICS

#ifdef USE_AVX512_INTRINSICS

__attribute__((target("avx512bw","avx512f"), optimize("no-tree-vectorize"))) 
size_t 
gen_bitmap_avx512_8(uint8_t* dst,
                    size_t* null_count_out,
                    uint8_t* src,
                    size_t size,
                    uint64_t null_val) {

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



__attribute__((target("avx512bw","avx512f"), optimize("no-tree-vectorize"))) 
size_t 
gen_bitmap_avx512_32(uint8_t* dst,
                    size_t* null_count_out,
                    uint32_t* src,
                    size_t size,
                    uint64_t null_val) {

  __m512i nulls_mask = _mm512_set1_epi64 ((int64_t)null_val); 

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

       
    // advancing *dst by 2 bytes, *src by 16 dwords (64 bytes)
    dst += 2;
    src += 16;

    // 64 bytes processed, decrementing count of remaining [u]int32_t by 16=64/4
    size -= 16;  // subq        $64, %rcx
  }

  *null_count_out = null_count;
  return null_count;
}

__attribute__((target("avx512bw","avx512f"), optimize("no-tree-vectorize"))) 
size_t
gen_bitmap_avx512_64(uint8_t* dst,
                    size_t* null_count_out,
                    uint64_t* src,
                    size_t size,
                    uint64_t null_val) {

  __m512i nulls_mask = _mm512_set1_epi64 ((int64_t)null_val); 

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
    ++dst;
    src += 64/8;
    size -= 8;    // 8 qwords processed, decrementing count of remaining qwords by 8
  }

  *null_count_out = null_count;
  return null_count;
}

#else 

size_t
__attribute__((target("default"))) 
gen_bitmap_avx512_8(uint8_t* dst,
                         size_t* null_count_out,
                         uint8_t* src,
                         size_t size,
                         uint64_t null_val) {

  uint8_t nulls_mask = null_val & 0xFF; 
  size_t null_count = 0;
  uint8_t loaded_bytes[8];

  while (size > 0) {
    uint8_t  encoded_byte = 0;
    memcpy(loaded_bytes, src, 8);

    for (size_t i = 0; i<8; i++) {
      uint8_t is_null = loaded_bytes[i] == nulls_mask;
      encoded_byte |= (!is_null) << i;
      null_count += is_null;
    }
    *dst = encoded_byte;

    dst += 1;
    src += 8;
    size -=8;
  }

  *null_count_out = null_count;
  return null_count;
}

size_t
__attribute__((target("default"))) 
gen_bitmap_avx512_32(uint8_t* dst,
                         size_t* null_count_out,
                         uint32_t* src,
                         size_t size,
                         uint64_t null_val) {

  uint32_t nulls_mask = null_val & 0xFFFFFFFF; 
  size_t null_count = 0;
  uint32_t loaded_words[8];
  while (size > 0) {
    uint8_t  encoded_byte = 0;
    memcpy(loaded_words, src, 8*4);

    for (size_t i = 0; i<8; i++) {
      uint8_t is_null = loaded_words[i] == nulls_mask;
      encoded_byte |= (!is_null) << i;
      null_count += is_null;
    }
    *dst = encoded_byte;

    dst += 1;
    src += 32/4;
    size -=32/4;
  }

  *null_count_out = null_count;
  return null_count;
}


size_t
__attribute__((target("default"))) 
gen_bitmap_avx512_64(uint8_t* dst,
                         size_t* null_count_out,
                         uint64_t* src,
                         size_t size,
                         uint64_t null_val) {

  uint64_t nulls_mask = null_val; 
  size_t null_count = 0;
  uint64_t loaded_qwords[8];
  while (size > 0) {
    uint8_t  encoded_byte = 0;
    memcpy(loaded_qwords, src, 8*8);

    for (size_t i = 0; i<8; i++) {
      uint8_t is_null = loaded_qwords[i] == nulls_mask;
      encoded_byte |= (!is_null) << i;
      null_count += is_null;
    }
    *dst = encoded_byte;

    dst += 1;
    src += 64/8;
    size -=64/8;
  }

  *null_count_out = null_count;
  return null_count;
}
#endif// USE_AVX512_INTRINSICS
