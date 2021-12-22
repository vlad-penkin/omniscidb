.text
        .globl generate_bitmap
        .p2align 4, 0x90
        .type generate_bitmap, @function

generate_bitmap: 
        # rdi: *dst
        # rsi: *null_count
        # rdx: *src
        # rcx: size 
        # r8: 
        # r9
        .cfi_startproc
#        vmovapd 0(%rdi), %zmm10
        vmovapd     0(%rdx), %zmm10         #   loading 512 bytes stored at pointer contained in rdx (3rd arg) into zmm10
        vpxorq      %zmm0, %zmm0, %zmm0     #   zmm0 <- 0
        # vpternlogd  %zmm0, %zmm0, %zmm0, 0xff

        # setting all bits of zmm0 to 1
#        mov         0xFFFFFFFF,  %eax
#        vpbroadcastd %eax, %zmm0

        vpcmpeqb    %zmm0, %zmm10, %k1     #   compares bytes stored in %zmm0 and %zmm10; sets corresponding bit in %k1
        kmovq	    %k1, %rax

        # saving bitmap to *dst
        movq        %rax, 0(%rdi)
        # counting nulls
        popcntl	    %eax, %eax
        movq    %rax, (%rsi)

#        .cfi_def_cfa_offset 8
        retq
        .cfi_endproc

