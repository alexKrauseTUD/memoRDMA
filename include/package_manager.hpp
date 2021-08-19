//
// Created by jpietrzyk on 13.08.21.
//

#ifndef TUDDBS_PACKAGEMAN_INCLUDE_PACKAGE_MANAGER_HPP
#define TUDDBS_PACKAGEMAN_INCLUDE_PACKAGE_MANAGER_HPP

#include <utility>
#include <cstddef>
#include <cstdint>
#include <cmath>
#include <cstring>
#include <string>

class package_t {
   public:
      using payload_size_t = std::size_t;
      using payload_t      = void;

      struct __attribute__((packed)) header_t {
         payload_size_t total_data_size; //this encodes the to-be-expected size of the total data received (in multiple packages).
         payload_size_t current_payload_size; //this encodes the size of the current payload in bytes.

         header_t(
            payload_size_t _total_data_size,
            payload_size_t _payload_size
         ):
            total_data_size{ _total_data_size },
            current_payload_size{ _payload_size }
         { }
      };
   
      [[nodiscard]] auto get_header() const {
         return header;
      }

      [[nodiscard]] auto get_payload() const {
         return payload;
      }
      
      [[nodiscard]] auto header_str() const {
         return
            " Total expected payload is " + std::to_string(header.total_data_size) + " bytes." + 
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
         payload_t *    _payload
      ) :
         header{  total_size, current_size },
         payload{ _payload } 
      {
      }

      virtual ~package_t() = default;
};


// struct package_manager_t {
//    // here we can put some organizational stuff maybe?
//    typename package_t::payload_size_t const max_payload_size_bytes = 4096;
//    double const max_pl_size_bytes_d = static_cast< double >( max_payload_size_bytes );
//    std::size_t package_id = 0;
   
   
//    void send_data( typename package_t::payload_t * data, typename package_t::payload_size_t size_in_bytes ) {
//       auto this_package_chunk_id = package_id++;
//       auto package_count =
//          static_cast< std::size_t >( std::ceil( static_cast< double>( size_in_bytes ) / max_pl_size_bytes_d ) );
      
//       std::size_t remainder_size_bytes = size_in_bytes - ( ( package_count - 1 ) * max_payload_size_bytes );
      
//       std::size_t package_no = 0;
      
//       package_t init_package( op_code_t::INIT, this_package_chunk_id, package_no++ );
//       //send init_package here!!!
//       std::cout << "Init Package: " << init_package.header_str() << "\n";
   
      
//       for( ; package_no < package_count; ++package_no ) {
//          package_t p( op_code_t::SENDING, this_package_chunk_id, package_no, max_payload_size_bytes, data );
//          //send package p here!!!
//          std::cout << "Sending...: " << p.header_str() << "\n";
//          data = reinterpret_cast< void * >( reinterpret_cast< char * >( data ) + max_payload_size_bytes );
//       }
//       if( remainder_size_bytes != 0 ) {
//          package_t r( op_code_t::SENDING, this_package_chunk_id, package_no++, remainder_size_bytes, data );
//          //send package r here!!!
//          std::cout << "Sending...: " << r.header_str() << "\n";
//       }
      
//       package_t closing_package( op_code_t::FINISHED, this_package_chunk_id, package_no );
//       //send closing_package here!!!
//       std::cout << "Closing Package...: " << closing_package.header_str() << "\n";
//    }
// };

#endif //TUDDBS_PACKAGEMAN_INCLUDE_PACKAGE_MANAGER_HPP
