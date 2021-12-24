CXX=g++
CXXFLAGS=-O2 -std=c++17  -I /localdisk2/gnovichk/miniconda3/envs/omnisci-dev/include
all:	bitmap

bitmap:	bitmap_vec.o bitmap.o 
	@echo "Building:\t$^ -> $@"
	@$(CXX) $^ -L /localdisk2/gnovichk/miniconda3/envs/omnisci-dev/lib -ltbb -o $@

%.o: %.cpp
	@echo "Compiling:\t$^"
	@$(CXX) $(CXXFLAGS) $^ -c

%.o: %.S
	@echo "Compiling:\t$^"
	@$(AS)  $^ -o $@

.PHONY: bitmap 


clean:
	rm *.o