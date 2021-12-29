CXX:=g++
CXXFLAGS:=-O3 -std=c++17  \
	-I /localdisk2/gnovichk/miniconda3/envs/omnisci-dev/include \
	-I ./ThirdParty/googletest \
	-I . 
LDDFLAGS:=\
	-L./build/ThirdParty/googletest/ \
	-L /localdisk2/gnovichk/miniconda3/envs/omnisci-dev/lib

all:	avxbmp.b avxbmp.t

avxbmp.b:	avx_gen_bitmap.o avxbmp.b.o avxbmp.o avxbmp_intr.o
	@echo "Building:\t$^ -> $@"
	@$(CXX) $^ $(LDDFLAGS) -ltbb -o $@

avxbmp.t:	avx_gen_bitmap.o avxbmp.t.o avxbmp.o avxbmp_intr.o
	@echo "Building:\t$^ -> $@"
	@$(CXX) $^ $(LDDFLAGS) -ltbb -lgtest -lpthread -o $@

intr: avxbmp_intr.cpp
	$(CXX) -O3 -S $^ 
#	$(CXX) -O0 -S $^ 
	cat avxbmp_intr.s

%.o: %.cpp
	@echo "Compiling:\t$^"
	@$(CXX) $(CXXFLAGS) $^ -c

%.o: %.S
	@echo "Compiling:\t$^"
	@$(AS)  $^ -o $@

.PHONY: all 

clean:
	rm -f avxbmp.b  avxbmp.t *.o  