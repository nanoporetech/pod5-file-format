def encode_table_row(control):
    table = []
    for i in range(7):
        offset = i * 2
        # first byte
        table.append(offset)
        if (control >> i) & 1:
            table.append(offset + 1)
    final_offset = 14
    for j in range(2):
        table.append(final_offset + j)
    for i in range(16 - len(table)):
        table.append(0xFF)
    return table


def decode_table_row(control):
    table = []
    offset = 0
    for i in range(8):
        table.append(offset)
        offset += 1
        if (control >> i) & 1:
            table.append(offset)
            offset += 1
        else:
            table.append(0xFF)
    return table


def print_x64_encode_table():
    print("static constexpr uint8_t g_encode_shuffle_table[128*16] = {")
    for i in range(128):
        table = encode_table_row(i)
        print("\t", ", ".join(f"0x{v:02X}" for v in table), ",", sep="")
    print("};\n\n")


def print_x64_decode_table():
    print("static const uint8_t g_decode_shuffle_table[256][16] = {")
    for i in range(256):
        table = decode_table_row(i)
        print("\t{ ", ", ".join(f"0x{v:02X}" for v in table), "},", sep="")
    print("};\n\n")


if __name__ == "__main__":
    print("#pragma once")
    print('#include "common.hpp" // arch macros')
    print("#include <cstdint>")
    print()
    print("#ifdef SVB16_X64")
    print_x64_encode_table()
    print_x64_decode_table()
    print("#endif")
