//
// Created by jpietrzyk on 13.08.21.
//

#ifndef TUDDBS_PACKAGEMAN_INCLUDE_PACKAGE_MANAGER_HPP
#define TUDDBS_PACKAGEMAN_INCLUDE_PACKAGE_MANAGER_HPP

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <utility>

class package_t {
   public:
    using payload_size_t = uint64_t;
    using payload_t = void;

    struct __attribute__((packed)) header_t {
        payload_size_t id;
        payload_size_t current_payload_size;  // this encodes the size of the current payload in bytes.
        payload_size_t package_number;
        payload_size_t data_type;
        payload_size_t total_data_size;       // this encodes the to-be-expected size of the total data received (in multiple packages).

        header_t() : id{0},
                     current_payload_size{0},
                     package_number{0},
                     data_type{0},
                     total_data_size{0} {}

        explicit header_t(
            payload_size_t _id,
            payload_size_t _payload_size,
            payload_size_t _package_number,
            payload_size_t _data_type,
            payload_size_t _total_data_size) : id{_id},
                                               current_payload_size{_payload_size},
                                               package_number{_package_number},
                                               data_type{_data_type},
                                               total_data_size{_total_data_size} {}
    };

    [[nodiscard]] auto& get_header() const {
        return header;
    }

    [[nodiscard]] auto get_payload() const {
        return payload;
    }

    [[nodiscard]] auto header_str() const {
        return "Total data size is " + std::to_string(header.total_data_size) + " bytes.\n" +
               "Carrying " + std::to_string(header.current_payload_size) + " bytes.\n" + 
               "Package id: " + std::to_string(header.id) + "\n" + 
               "Package Number: " + std::to_string(header.package_number) + "\n" +
               "Data Type: " + std::to_string(header.data_type) + "\n";
    }

   private:
    header_t header;
    payload_t* payload;

   public:
    package_t() = delete;
    package_t(
        payload_size_t id,
        payload_size_t current_size,
        payload_size_t package_number,
        payload_size_t data_type,
        payload_size_t total_size,
        payload_t* _payload) : header{id, current_size, package_number, data_type, total_size},
                               payload{_payload} {
    }

    package_t(package_t& other) : header(other.header), payload(other.payload) {

    }

    // package_t& operator=(package_t other)
    // {
    //     std::swap(header, other.header);
    //     std::swap(payload, other.payload);
    //     return *this;
    // }

    package_t* deep_copy() const {
        return new package_t( header.id, header.current_payload_size, header.package_number, (uint64_t)header.data_type, header.total_data_size, payload );
    }

    void setCurrentPackageSize(const std::size_t bytes) {
        header.current_payload_size = bytes;
    }

    void setCurrentPackageNumber(const std::size_t num) {
        header.package_number = num;
    }

    void advancePayloadPtr(const std::size_t bytes) {
        payload = (void*)((char*)payload + bytes);
    }

    static std::size_t metaDataSize() {
        return sizeof(header_t);
    }

    std::size_t packageSize() const {
        return metaDataSize() + header.current_payload_size;
    }

    virtual ~package_t() = default;
};

#endif  // TUDDBS_PACKAGEMAN_INCLUDE_PACKAGE_MANAGER_HPP
