#ifndef MINISQL_FIELD_H
#define MINISQL_FIELD_H

#include <cstring>
#include <string>

#include "common/config.h"
#include "common/macros.h"
#include "record/type_id.h"
#include "record/types.h"

/**
* \brief 域：对应于一条记录中某一个字段的数据信息,如存储数据的数据类型，是否是空，存储数据的值等
*/
class Field {
	// 定义友元类，Type等可以是同Field的成员函数
  friend class Type;

  friend class TypeInt;

  friend class TypeChar;

  friend class TypeFloat;

 public:
  explicit Field(const TypeId type) : type_id_(type), len_(FIELD_NULL_LEN), is_null_(true) {}

  ~Field() {
		// char类型的数据存放在程序手动new的内存中，所以要单独释放
    if (type_id_ == TypeId::kTypeChar && manage_data_) {
      delete[] value_.chars_;
    }
  }

  // integer
  explicit Field(TypeId type, int32_t i) : type_id_(type) {
    ASSERT(type == TypeId::kTypeInt, "Invalid type.");
    value_.integer_ = i;
    len_ = Type::GetTypeSize(type);
  }

  // float
  explicit Field(TypeId type, float f) : type_id_(type) {
    ASSERT(type == TypeId::kTypeFloat, "Invalid type.");
    value_.float_ = f;
    len_ = Type::GetTypeSize(type);
  }

  // char
  explicit Field(TypeId type, char *data, uint32_t len, bool manage_data) : type_id_(type), manage_data_(manage_data) {
    ASSERT(type == TypeId::kTypeChar, "Invalid type.");
    if (data == nullptr) {
      is_null_ = true;
      len_ = 0;
      value_.chars_ = nullptr;
      manage_data_ = false;
    } else {
      if (manage_data) {
				// 在len >= VARCHAR_MAX_LEN的时候会终止程序
        ASSERT(len < VARCHAR_MAX_LEN, "Field length exceeds max varchar length");
        value_.chars_ = new char[len];
        memcpy(value_.chars_, data, len);
      } else {
        value_.chars_ = data;
      }
      len_ = len;
    }
  }

  // copy constructor
  explicit Field(const Field &other) {
    type_id_ = other.type_id_;
    len_ = other.len_;
    is_null_ = other.is_null_;
    manage_data_ = other.manage_data_;
    if (type_id_ == TypeId::kTypeChar && !is_null_ && manage_data_) {
      value_.chars_ = new char[len_];
      memcpy(value_.chars_, other.value_.chars_, len_);
    } else {
      value_ = other.value_;
    }
  }

  // copy
  Field &operator=(Field &other) {
    Swap(*this, other);
    return *this;
  }

  inline bool IsNull() const { return is_null_; }

  inline uint32_t GetLength() const { return Type::GetInstance(type_id_)->GetLength(*this); }

  inline TypeId GetTypeId() const { return type_id_; }

  inline const char *GetData() const { return Type::GetInstance(type_id_)->GetData(*this); }

  inline uint32_t SerializeTo(char *buf) const { return Type::GetInstance(type_id_)->SerializeTo(*this, buf); }

  inline static uint32_t DeserializeFrom(char *buf, const TypeId type_id, Field **field, bool is_null) {
    return Type::GetInstance(type_id)->DeserializeFrom(buf, field, is_null);
  }

  inline uint32_t GetSerializedSize() const { return Type::GetInstance(type_id_)->GetSerializedSize(*this, is_null_); }

	// 判断数据类型是否相同
  inline bool CheckComparable(const Field &o) const { return type_id_ == o.type_id_; }

	// 下面是实现两个相同数据类型的对象的比较：==,!=,<,<=,>,>=
  inline CmpBool CompareEquals(const Field &o) const { return Type::GetInstance(type_id_)->CompareEquals(*this, o); }

  inline CmpBool CompareNotEquals(const Field &o) const {
    return Type::GetInstance(type_id_)->CompareNotEquals(*this, o);
  }

  inline CmpBool CompareLessThan(const Field &o) const {
    return Type::GetInstance(type_id_)->CompareLessThan(*this, o);
  }

  inline CmpBool CompareLessThanEquals(const Field &o) const {
    return Type::GetInstance(type_id_)->CompareLessThanEquals(*this, o);
  }

  inline CmpBool CompareGreaterThan(const Field &o) const {
    return Type::GetInstance(type_id_)->CompareGreaterThan(*this, o);
  }

  inline CmpBool CompareGreaterThanEquals(const Field &o) const {
    return Type::GetInstance(type_id_)->CompareGreaterThanEquals(*this, o);
  }

  friend void Swap(Field &first, Field &second) {
		// 交换两个对象的值
    std::swap(first.value_, second.value_);
    std::swap(first.type_id_, second.type_id_);
    std::swap(first.len_, second.len_);
    std::swap(first.is_null_, second.is_null_);
    std::swap(first.manage_data_, second.manage_data_);
  }

  std::string toString() {
    if (is_null_)
      return "NULL";
    else if (type_id_ == kTypeInt)
      return std::to_string(value_.integer_);
    else if (type_id_ == kTypeFloat)
      return std::to_string(value_.float_);
    else {
      char temp[len_ + 1];
      memcpy(temp, value_.chars_, len_);
      temp[len_] = '\0';
			// {}实现列表初始化，参数是字符指针，返回std::string
      return {temp};
    }
  }

 protected:
	// 实现三者共用一块内存
  union Val {
    int32_t integer_;
    float float_;
    char *chars_;
  } value_;
  TypeId type_id_;
  uint32_t len_;
  bool is_null_{false};
  bool manage_data_{false}; // 是否具有数据的所有权
};

#endif  // MINISQL_FIELD_H
