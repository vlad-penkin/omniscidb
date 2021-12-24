CXX=g++
CXXFLAGS=-O2 -std=c++17  -I /localdisk2/gnovichk/miniconda3/envs/omnisci-dev/include -I .

all:	avxbmp.b avxbmp.t

avxbmp.b:	avx_gen_bitmap.o avxbmp.b.o avxbmp.o
	@echo "Building:\t$^ -> $@"
	@$(CXX) $^ -L /localdisk2/gnovichk/miniconda3/envs/omnisci-dev/lib -ltbb -o $@

avxbmp.t:	avx_gen_bitmap.o avxbmp.t.o avxbmp.o
	@echo "Building:\t$^ -> $@"
	@$(CXX) $^ -L /localdisk2/gnovichk/miniconda3/envs/omnisci-dev/lib -ltbb -o $@


%.o: %.cpp
	@echo "Compiling:\t$^"
	@$(CXX) $(CXXFLAGS) $^ -c

%.o: %.S
	@echo "Compiling:\t$^"
	@$(AS)  $^ -o $@

.PHONY: all 

clean:
	rm -f avxbmp.b  avxbmp.t *.o  