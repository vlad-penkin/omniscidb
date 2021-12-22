.text
        .globl gen_bitmap_8
        .p2align 4, 0x90
        .type gen_bitmap_8, @function

gen_bitmap_8: 
        # rdi: *dst
        # rsi: *null_count
        # rdx: *src
        # rcx: size 
        .cfi_startproc

        # setting all bits of zmm0 to 1
        mov         $0xFFFFFFFF,  %eax
        vpbroadcastd %eax, %zmm0

        # loading 512 bytes stored at pointer contained in rdx (3rd arg) into zmm10
        vmovapd     0(%rdx), %zmm10

        # comparing 0xFF bytes (nulls) stored in %zmm0 and %zmm10; sets not  bit in %k1
        vpcmpneqb   %zmm0, %zmm10, %k1     
        kmovq	    %k1, %rax

        # saving 64-bit bitmap to *dst
        movq        %rax, 0(%rdi)

        # counting nulls
        popcntl	    %eax, %eax
        movq    %rax, (%rsi)

#        .cfi_def_cfa_offset 8
        retq
        .cfi_endproc

