.text
        .globl gen_bitmap_8
        .p2align 4, 0x90
        .type gen_bitmap_8, @function

gen_bitmap_8: 
        # rdi: *dst
        # rsi: *null_count
        # rdx: *src
        # rcx: size 
        # r8:  nullvalues (byte-repeated) for the type
        .cfi_startproc

        xorq        %r9, %r9

        # broadcasting to zmm0 int8_t/uint8_t null values (repeated to be 64 bit), 
        # stored in r8 (5th argument)
        vpbroadcastq  %r8, %zmm0

.Lloop_start_gbm8:
        # loading 512 bytes stored at pointer contained in rdx (3rd arg) into zmm10
        vmovapd     0(%rdx), %zmm10

        # comparing 0xFF bytes (nulls) stored in %zmm0 and %zmm10; sets not  bit in %k1
        vpcmpneqb   %zmm0, %zmm10, %k1     
        kmovq	    %k1, %rax

        # saving 64-bit bitmap to *dst
        movq        %rax, 0(%rdi)

        # counting nulls, storing nulls count to %rax
        not         %rax
        popcnt	    %rax, %rax
        addq        %rax, %r9

        addq        $8,  %rdi
        addq        $64, %rdx

        subq        $64, %rcx
        cmp         $0, %rcx

        jg          .Lloop_start_gbm8

        # storing nulls count
        movq        %r9, (%rsi)
#        .cfi_def_cfa_offset 8
        retq
        .cfi_endproc


.text
        .globl gen_bitmap_32
        .p2align 4, 0x90
        .type gen_bitmap_32, @function

gen_bitmap_32: 
        # rdi: *dst
        # rsi: *null_count
        # rdx: *src
        # rcx: size 
        # r8:  nullvalues (dword-repeated) for the type
        .cfi_startproc

        xorq        %r9, %r9

        # broadcasting to zmm0 int32_t/uint32_t null values (repeated to be 64 bit), 
        # stored in r8 (5th argument)
        vpbroadcastq  %r8, %zmm0

.Lloop_start_gbm32:
        # loading 512 bytes stored at pointer contained in rdx (3rd arg) into zmm10
        vmovapd     0(%rdx), %zmm10

        # comparing repeated nulls stored in %zmm0 and %zmm10; sets not  bit in %k1
        vpcmpneqd   %zmm0, %zmm10, %k1     
        kmovq	    %k1, %rax

        # saving 16-bit bitmap to *dst (64 bytes of input/4 byte-long double word yield 16 comparisons, hence 16 bits)
        mov         %ax, 0(%rdi)
        # advance output pointer by 2 bytes
        addq        $2,  %rdi
        # advance input pointer by 64 bytes
        addq        $64, %rdx

        # counting nulls, storing nulls count to %eax
        not         %ax
        popcntl	    %eax, %eax
        addq        %rax, %r9

        # 16 dwords processed, decrementing count of remaining dwords by 16
        subq        $16, %rcx
        cmp         $0, %rcx

        jg          .Lloop_start_gbm32

        # storing nulls count
        movq        %r9, (%rsi)
#        .cfi_def_cfa_offset 8
        retq
        .cfi_endproc

#
#
#
.text
        .globl gen_bitmap_64
        .p2align 4, 0x90
        .type gen_bitmap_64, @function

gen_bitmap_64: 
        # rdi: *dst
        # rsi: *null_count
        # rdx: *src
        # rcx: size 
        .cfi_startproc

        xorq        %r9, %r9

        # broadcasting to zmm0 int64_t/uint64_t/double null values (repeated to be 64 bit), 
        # stored in r8 (5th argument)
        vpbroadcastq  %r8, %zmm0

.Lloop_start_gbm64:
        # loading 512 bytes stored at pointer contained in rdx (3rd arg) into zmm10
        vmovapd     0(%rdx), %zmm10

        # comparing repeated nulls stored in %zmm0 and %zmm10; not equal bit are in %k1
        vpcmpneqq   %zmm0, %zmm10, %k1     
        kmovq	    %k1, %rax

        # saving 8-bit bitmap to *dst (64 bytes of input/8 byte-long quad word yield 8 comparisons, hence 8 bits)
        mov         %al, 0(%rdi)
        # advance output pointer by 1 byte
        inc         %rdi
        # advance input pointer by 64 bytes
        addq        $64, %rdx

        # counting nulls, storing nulls count to %rax
        not         %al
        popcntl	    %eax, %eax
        addq        %rax, %r9

        # 8 qwords processed, decrementing count of remaining qwords by 8
        subq        $8, %rcx
        cmp         $0, %rcx

        jg          .Lloop_start_gbm64

        # storing nulls count
        movq        %r9, (%rsi)
#        .cfi_def_cfa_offset 8
        retq
        .cfi_endproc

