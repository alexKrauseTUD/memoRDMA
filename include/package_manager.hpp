//
// Created by jpietrzyk on 13.08.21.
//

/**
 * @brief SAMPLE usage:
 *    auto pl = malloc( 300 );
   package_manager_t pm;
   pm.send_data( pl, 300 );
   
   
   auto pl2 = malloc( 3000000 );
   pm.send_data( pl2, 3000000 );
 */
#ifndef TUDDBS_PACKAGEMAN_INCLUDE_PACKAGE_MANAGER_HPP
#define TUDDBS_PACKAGEMAN_INCLUDE_PACKAGE_MANAGER_HPP

#include <utility>
#include <cstddef>
#include <cstdint>
#include <cmath>
#include <cstring>

enum class op_code_t : uint8_t {
   INIT = 1,
   SENDING = 2,
   FINISHED = 3
};

class package_t {
   public:
      using package_id_t   = std::size_t;
      using package_no_t   = std::size_t;
      using payload_size_t = std::size_t;
      using payload_t      = void;
   private:
      struct __attribute__((packed)) header_t {
         op_code_t      op_code;
         package_id_t   package_id; //this is used to assign a specific package to a package-chunk
         package_no_t   package_no; //this encodes the package number within a package-chunk
         payload_size_t payload_size; //this encodes the size of the payload in bytes.
         //Do we need things like IP, or CRC?
         header_t() = delete;
         explicit header_t( void * header_data ) :
            op_code{       reinterpret_cast< header_t * >( header_data )->op_code      },
            package_id{    reinterpret_cast< header_t * >( header_data )->package_id   },
            package_no{    reinterpret_cast< header_t * >( header_data )->package_no   },
            payload_size{  reinterpret_cast< header_t * >( header_data )->payload_size }
         { }
         header_t(
            op_code_t _opcode, package_id_t _package_id, package_no_t _package_no, payload_size_t _payload_size
         ):
           op_code{        _opcode        },
           package_id{     _package_id    },
           package_no{     _package_no    },
           payload_size{   _payload_size  }
         { }
      };
   
   public:
      [[nodiscard]] auto get_header() const {
         return header;
      }
      [[nodiscard]] auto get_payload() const {
         return payload;
      }
      
      [[nodiscard]] auto header_str() const {
         return
            "Package " + std::to_string(header.package_no) +
            " with Id " + std::to_string(header.package_id) +
            ". OP-Code " + std::to_string( static_cast< int >(header.op_code)) +
            ". Carrying " + std::to_string(header.payload_size) + " bytes.";
      }
      
   private:
      header_t     header;
      payload_t *  payload;
   public:
      package_t() = delete;
      package_t(
         op_code_t      op_code,
         package_id_t   package_id,
         package_no_t   package_no,
         payload_size_t payload_size,
         payload_t *    _payload
      ) :
         header{ op_code, package_id, package_no, payload_size },
         payload{ malloc( payload_size) } {
         std::memcpy( payload, _payload, payload_size );
      }
         
      explicit package_t(
         void * package
      ) :
         header{ package },
         payload{ reinterpret_cast< payload_t * >( reinterpret_cast< char * >( package ) + sizeof( header_t ) ) }
      { }
      explicit package_t(
         op_code_t op_code,
         package_id_t   package_id,
         package_no_t   package_no
      ) :
         header{ op_code, package_id, package_no, 0 },
         payload{ nullptr } { }
         
      virtual ~package_t() {
         if( header.op_code == op_code_t::SENDING ) {
            free( payload );
         }
      }
};


struct package_manager_t {
   // here we can put some organizational stuff maybe?
   typename package_t::payload_size_t const max_payload_size_bytes = 4096;
   double const max_pl_size_bytes_d = static_cast< double >( max_payload_size_bytes );
   std::size_t package_id = 0;
   
   
   void send_data( typename package_t::payload_t * data, typename package_t::payload_size_t size_in_bytes ) {
      auto this_package_chunk_id = package_id++;
      auto package_count =
         static_cast< std::size_t >( std::ceil( static_cast< double>( size_in_bytes ) / max_pl_size_bytes_d ) );
      
      std::size_t remainder_size_bytes = size_in_bytes - ( ( package_count - 1 ) * max_payload_size_bytes );
      
      std::size_t package_no = 0;
      
      package_t init_package( op_code_t::INIT, this_package_chunk_id, package_no++ );
      //send init_package here!!!
      std::cout << "Init Package: " << init_package.header_str() << "\n";
   
      
      for( ; package_no < package_count; ++package_no ) {
         package_t p( op_code_t::SENDING, this_package_chunk_id, package_no, max_payload_size_bytes, data );
         //send package p here!!!
         std::cout << "Sending...: " << p.header_str() << "\n";
         data = reinterpret_cast< void * >( reinterpret_cast< char * >( data ) + max_payload_size_bytes );
      }
      if( remainder_size_bytes != 0 ) {
         package_t r( op_code_t::SENDING, this_package_chunk_id, package_no++, remainder_size_bytes, data );
         //send package r here!!!
         std::cout << "Sending...: " << r.header_str() << "\n";
      }
      
      package_t closing_package( op_code_t::FINISHED, this_package_chunk_id, package_no );
      //send closing_package here!!!
      std::cout << "Closing Package...: " << closing_package.header_str() << "\n";
   }
};

#endif //TUDDBS_PACKAGEMAN_INCLUDE_PACKAGE_MANAGER_HPP
