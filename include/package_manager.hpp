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
    using payload_size_t = std::size_t;
    using payload_t = void;

    struct __attribute__((packed)) header_t {
        payload_size_t total_data_size;       // this encodes the to-be-expected size of the total data received (in multiple packages).
        payload_size_t current_payload_size;  // this encodes the size of the current payload in bytes.

        header_t() : total_data_size{0},
                     current_payload_size{0} {}

        explicit header_t(
            payload_size_t _total_data_size,
            payload_size_t _payload_size) : total_data_size{_total_data_size},
                                            current_payload_size{_payload_size} {}
    };

    [[nodiscard]] auto& get_header() const {
        return header;
    }

    [[nodiscard]] auto get_payload() const {
        return payload;
    }

    [[nodiscard]] auto header_str() const {
        return " Total expected payload is " + std::to_string(header.total_data_size) + " bytes." +
               ". Carrying " + std::to_string(header.current_payload_size) + " bytes.";
    }

   private:
    header_t header;
    payload_t* payload;

   public:
    package_t() = delete;
    package_t(
        payload_size_t total_size,
        payload_size_t current_size,
        payload_t* _payload) : header{total_size, current_size},
                               payload{_payload} {
    }

    void setCurrentPackageSize(const std::size_t bytes) {
        header.current_payload_size = bytes;
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
