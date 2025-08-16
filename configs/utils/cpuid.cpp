
#include <cstdint>
#include <iostream>
#include <cstring>

#if defined(__i386__) || defined(__x86_64__)
inline void cpuid(unsigned int EAX_input, unsigned int* eax, unsigned int* ebx, unsigned int* ecx, unsigned int* edx) {
    asm volatile(
        "cpuid"
        : "=a" (*eax), "=b" (*ebx), "=c" (*ecx), "=d" (*edx)
        : "a" (EAX_input)
    );
}
#else
inline void cpuid(unsigned int EAX_input, unsigned int* eax, unsigned int* ebx, unsigned int* ecx, unsigned int* edx) {
    *eax = *ebx = *ecx = *edx = 0;
}
#endif

inline void usage() {
    std::cerr << "Usage: cpuid <EAX_input> <Register> <Bit>" << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 4) {
        usage();
        return 1;
    }
    unsigned int eax, ebx, ecx, edx;
    unsigned int EAX_input = std::stoi(argv[1]);
    char* target_register = argv[2];
    unsigned int bit = std::stoi(argv[3]);
    if (target_register == nullptr) {
        usage();
        return 1;
    } else if ((strcmp(target_register, "EAX") != 0) && (strcmp(target_register, "EBX") != 0) &&
               (strcmp(target_register, "ECX") != 0) && (strcmp(target_register, "EDX") != 0)) {
        usage();
        return 1;
    }

    if (bit > 31) {
        usage();
        return 1;
    }

    cpuid(EAX_input, &eax, &ebx, &ecx, &edx);
    if (strcmp(target_register, "EAX") == 0) {
        std::cout << ((eax >> bit) & 1) << std::endl;
    } else if (strcmp(target_register, "EBX") == 0) {
        std::cout << ((ebx >> bit) & 1) << std::endl;
    } else if (strcmp(target_register, "ECX") == 0) {
        std::cout << ((ecx >> bit) & 1) << std::endl;
    } else if (strcmp(target_register, "EDX") == 0) {
        std::cout << ((edx >> bit) & 1) << std::endl;
    }

    return 0;
}

