#ifndef MINISQL_MACROS_H
#define MINISQL_MACROS_H

#include <cassert>

#define ASSERT(expr, message) assert((expr) && (message))

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)                             \
  cname(const cname &) = delete;            /* NOLINT */ \
  cname &operator=(const cname &) = delete; /* NOLINT */

#define DISALLOW_MOVE(cname)                        \
  cname(cname &&) = delete;            /* NOLINT */ \
  cname &operator=(cname &&) = delete; /* NOLINT */

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

// 往内存（由Buf指定的位置）中写入一个指定类型(Type)的数据(Data)
// reinterpret_cast强制类型转换
#define MACH_WRITE_TO(Type, Buf, Data)       \
  do {                                       \
    *reinterpret_cast<Type *>(Buf) = (Data); \
  } while (0)
#define MACH_WRITE_UINT32(Buf, Data) MACH_WRITE_TO(uint32_t, (Buf), (Data))
#define MACH_WRITE_INT32(Buf, Data) MACH_WRITE_TO(int32_t, (Buf), (Data))
#define MACH_WRITE_STRING(Buf, Str)         \
  do {                                      \
    memcpy(Buf, Str.c_str(), Str.length()); \
  } while (0)

// 将内存(由Buf指定的位置)转化成指定类型(Type)的数据
#define MACH_READ_FROM(Type, Buf) (*reinterpret_cast<const Type *>(Buf))
// 将内存(由Buf指定的位置)转化成uint32_t类型的数据
#define MACH_READ_UINT32(Buf) MACH_READ_FROM(uint32_t, (Buf))
// 将内存(由Buf指定的位置)转化成int32_t类型的数据
#define MACH_READ_INT32(Buf) MACH_READ_FROM(int32_t, (Buf))

#define MACH_STR_SERIALIZED_SIZE(Str) (4 + Str.length())

#define ALLOC(Heap, Type) new (Heap.Allocate(sizeof(Type))) Type
#define ALLOC_P(Heap, Type) new (Heap->Allocate(sizeof(Type))) Type
#define ALLOC_COLUMN(Heap) ALLOC(Heap, Column)

#endif  // MINISQL_MACROS_H
