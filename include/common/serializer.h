#pragma once
#include <cstdint>
#include <cstring>

enum CODE_TYPE {
    CODE_ROWID = 1,
    CODE_INT32,
    CODE_UINT32,
    CODE_INT64,
    CODE_UINT64,
    CODE_FLOAT,
    CODE_VARCHAR,
    CODE_INVALID = 255,
};

static const uint32_t INT32_SIGN = 0x80000000;
static const uint64_t INT64_SIGN = 0x8000000000000000;

class BinaryWriter {
private:
     char* buffer;    // 缓冲区指针
    size_t position; // 当前写入位置

public:
    inline explicit BinaryWriter( char* data) : buffer(data), position(0) {}

    // 获取已写入的数据大小
    inline size_t get_size() const { return position; }

    // 写入无符号8位整数
    inline void write_uint8(uint8_t value) {
        buffer[position++] = static_cast<char>(value);
    }

    // 写入有符号8位整数 (符号位翻转以保持排序)
    inline void write_int8(int8_t value) { write_uint8(static_cast<uint8_t>(value) ^ 0x80); }

    // 写入无符号16位整数(大端序)
    inline void write_uint16(uint16_t value) {
        buffer[position++] = static_cast<char>((value >> 8) & 0xFF);
        buffer[position++] = static_cast<char>(value & 0xFF);
    }

    // 写入有符号16位整数(大端序，符号位翻转)
    inline void write_int16(int16_t value) { write_uint16(static_cast<uint16_t>(value) ^ 0x8000); }

    // 写入无符号32位整数(大端序)
    inline void write_uint32(uint32_t value) {
        buffer[position++] = static_cast<char>((value >> 24) & 0xFF);
        buffer[position++] = static_cast<char>((value >> 16) & 0xFF);
        buffer[position++] = static_cast<char>((value >> 8) & 0xFF);
        buffer[position++] = static_cast<char>(value & 0xFF);
    }

    // 写入有符号32位整数(大端序，符号位翻转)
    inline void write_int32(int32_t value) { write_uint32(static_cast<uint32_t>(value) ^ INT32_SIGN); }

    // 写入无符号64位整数(大端序)
    inline void write_uint64(uint64_t value) {
        buffer[position++] = static_cast<char>((value >> 56) & 0xFF);
        buffer[position++] = static_cast<char>((value >> 48) & 0xFF);
        buffer[position++] = static_cast<char>((value >> 40) & 0xFF);
        buffer[position++] = static_cast<char>((value >> 32) & 0xFF);
        buffer[position++] = static_cast<char>((value >> 24) & 0xFF);
        buffer[position++] = static_cast<char>((value >> 16) & 0xFF);
        buffer[position++] = static_cast<char>((value >> 8) & 0xFF);
        buffer[position++] = static_cast<char>(value & 0xFF);
    }

    // 写入有符号64位整数(大端序，符号位翻转)
    inline void write_int64(int64_t value) { write_uint64(static_cast<uint64_t>(value) ^ INT64_SIGN); }

    // 写入字符串(不包含长度信息和结束字符)
    inline void write_string(const char* str, size_t length) {
        memcpy(buffer + position, str, length);
        position += length;
    }

    inline void write_string(const char* str) {
        const size_t len = strlen(str);
        write_string(str, len);
    }

    // 写入原始字节数据
    inline void write_bytes(const void* data, size_t length) {
        memcpy(buffer + position, data, length);
        position += length;
    }
};

class BinaryReader {
private:
    const char* buffer;  // 缓冲区指针
    size_t position;     // 当前读取位置

public:
    inline explicit BinaryReader(const char* data) : buffer(data), position(0) {}

    // 获取当前读取位置
    inline size_t get_position() const {
        return position;
    }

    // 设置读取位置
    inline void set_position(size_t pos) {
        position = pos;
    }

    // 读取无符号8位整数
    inline uint8_t read_uint8() {
        return static_cast<uint8_t>(buffer[position++]);
    }

    // 读取有符号8位整数 (符号位翻转)
    inline int8_t read_int8() {
        return static_cast<int8_t>(read_uint8() ^ 0x80);
    }

    // 读取无符号16位整数(大端序)
    inline uint16_t read_uint16() {
        uint16_t value = static_cast<uint16_t>(static_cast<uint8_t>(buffer[position])) << 8;
        value |= static_cast<uint16_t>(static_cast<uint8_t>(buffer[position + 1]));
        position += 2;
        return value;
    }

    // 读取有符号16位整数(大端序，符号位翻转)
    inline int16_t read_int16() {
        return static_cast<int16_t>(read_uint16() ^ 0x8000);
    }

    // 读取无符号32位整数(大端序)
    inline uint32_t read_uint32() {
        uint32_t value = static_cast<uint32_t>(static_cast<uint8_t>(buffer[position])) << 24;
        value |= static_cast<uint32_t>(static_cast<uint8_t>(buffer[position + 1])) << 16;
        value |= static_cast<uint32_t>(static_cast<uint8_t>(buffer[position + 2])) << 8;
        value |= static_cast<uint32_t>(static_cast<uint8_t>(buffer[position + 3]));
        position += 4;
        return value;
    }

    // 读取有符号32位整数(大端序，符号位翻转)
    inline int32_t read_int32() {
        return static_cast<int32_t>(read_uint32() ^ INT32_SIGN);
    }
    // 读取无符号64位整数(大端序)
    inline uint64_t read_uint64() {
        uint64_t value = static_cast<uint64_t>(static_cast<uint8_t>(buffer[position])) << 56;
        value |= static_cast<uint64_t>(static_cast<uint8_t>(buffer[position + 1])) << 48;
        value |= static_cast<uint64_t>(static_cast<uint8_t>(buffer[position + 2])) << 40;
        value |= static_cast<uint64_t>(static_cast<uint8_t>(buffer[position + 3])) << 32;
        value |= static_cast<uint64_t>(static_cast<uint8_t>(buffer[position + 4])) << 24;
        value |= static_cast<uint64_t>(static_cast<uint8_t>(buffer[position + 5])) << 16;
        value |= static_cast<uint64_t>(static_cast<uint8_t>(buffer[position + 6])) << 8;
        value |= static_cast<uint64_t>(static_cast<uint8_t>(buffer[position + 7]));
        position += 8;
        return value;
    }

    // 读取有符号64位整数(大端序，符号位翻转)
    inline int64_t read_int64() {
        return static_cast<int64_t>(read_uint64() ^ INT64_SIGN);
    }

    // 读取指定长度的字符串到目标缓冲区
    inline void read_string(char* dest, size_t length) {
        read_bytes(dest, length);
    }

    // 读取原始字节数据
    inline void read_bytes(void* dest, size_t length) {
        memcpy(dest, buffer + position, length);
        position += length;
    }

    // 读取指定长度的数据但不移动读取位置
    inline void peek_bytes(void* dest, size_t length) const {
        memcpy(dest, buffer + position, length);
    }

    // 跳过指定字节数
    inline void skip(size_t count) {
        position += count;
    }
};

