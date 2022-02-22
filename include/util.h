#ifndef UTILS_H
#define UTILS_H

// #include <RDMARegion.h>
#include <assert.h>
#include <byteswap.h>
#include <endian.h>
#include <errno.h>
#include <getopt.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#define MAX_POLL_CQ_TIMEOUT 30000  // ms

#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

#define ERROR(fmt, args...)                                     \
    {                                                           \
        fprintf(stderr, "ERROR: %s(): " fmt, __func__, ##args); \
    }
#define ERR_DIE(fmt, args...) \
    {                         \
        ERROR(fmt, ##args);   \
        exit(EXIT_FAILURE);   \
    }
#define INFO(fmt, args...)                            \
    {                                                 \
        printf("INFO: %s(): " fmt, __func__, ##args); \
    }
#define WARN(fmt, args...)                            \
    {                                                 \
        printf("WARN: %s(): " fmt, __func__, ##args); \
    }

#define CHECK(expr)                  \
    {                                \
        int rc = (expr);             \
        if (rc != 0) {               \
            perror(strerror(errno)); \
            exit(EXIT_FAILURE);      \
        }                            \
    }

// structure of test parameters
struct config_t {
    std::string dev_name;     // IB device name
    std::string server_name;  // server hostname
    uint32_t tcp_port;        // server TCP port
    bool client_mode;         // Don't run an event loop
    int ib_port;              // local IB port to work with
    int gid_idx;              // GID index to use
};

struct buffer_config_t {
    unsigned int num_own_receive;
    std::size_t size_own_receive;
    unsigned int num_remote_receive;
    std::size_t size_remote_receive;
    std::size_t size_own_send;
    std::size_t size_remote_send;
};

static buffer_config_t invertBufferConfig(buffer_config_t bufferConfig) {
    return {.num_own_receive = bufferConfig.num_remote_receive,
            .size_own_receive = bufferConfig.size_remote_receive,
            .num_remote_receive = bufferConfig.num_own_receive,
            .size_remote_receive = bufferConfig.size_own_receive,
            .size_own_send = bufferConfig.size_remote_send,
            .size_remote_send = bufferConfig.size_own_send};
}

// \begin socket operation
//
// For simplicity, the example program uses TCP sockets to exchange control
// information. If a TCP/IP stack/connection is not available, connection
// manager (CM) may be used to pass this information. Use of CM is beyond the
// scope of this example.

// Connect a socket. If servername is specified a client connection will be
// initiated to the indicated server and port. Otherwise listen on the indicated
// port for an incoming connection.
static int sock_connect(std::string server_name, int port) {
    struct addrinfo *resolved_addr = NULL;
    struct addrinfo *iterator;
    char service[6];
    int sockfd = -1;
    int listenfd = 0;

    // @man getaddrinfo:
    //  struct addrinfo {
    //      int             ai_flags;
    //      int             ai_family;
    //      int             ai_socktype;
    //      int             ai_protocol;
    //      socklen_t       ai_addrlen;
    //      struct sockaddr *ai_addr;
    //      char            *ai_canonname;
    //      struct addrinfo *ai_next;
    //  }
    struct addrinfo hints = {.ai_flags = AI_PASSIVE,
                             .ai_family = AF_INET,
                             .ai_socktype = SOCK_STREAM,
                             .ai_protocol = 0};

    // resolve DNS address, user sockfd as temp storage
    sprintf(service, "%d", port);
    if (server_name.empty()) {
        CHECK(getaddrinfo(NULL, service, &hints, &resolved_addr));
    } else {
        CHECK(getaddrinfo(server_name.c_str(), service, &hints, &resolved_addr));
    }

    for (iterator = resolved_addr; iterator != NULL; iterator = iterator->ai_next) {
        sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
        assert(sockfd >= 0);

        if (server_name.empty()) {
            // Client mode: setup listening socket and accept a connection
            listenfd = sockfd;
            CHECK(bind(listenfd, iterator->ai_addr, iterator->ai_addrlen));
            CHECK(listen(listenfd, 1));
            sockfd = accept(listenfd, NULL, 0);
        } else {
            // Server mode: initial connection to remote
            CHECK(connect(sockfd, iterator->ai_addr, iterator->ai_addrlen));
        }
    }

    return sockfd;
}

static bool is_port_free(int port, char *host = "localhost") {
    int sockfd;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        ERROR("ERROR opening socket");
    }

    server = gethostbyname(host);

    if (server == NULL) {
        fprintf(stderr, "ERROR, no such host\n");
        exit(0);
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr,
          (char *)&serv_addr.sin_addr.s_addr,
          server->h_length);

    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sockfd);
        return false;
    } else {
        close(sockfd);
        return true;
    }
}

static void sock_close(int &sockfd) {
    close(sockfd);
}

static int receive_tcp(int sockfd, int xfer_size, char *remote_data) {
    int read_bytes = 0;

    read_bytes = read(sockfd, remote_data, xfer_size);
    assert(read_bytes == xfer_size);

    // FIXME: hard code that always returns no error
    return 0;
}

static int send_tcp(int sockfd, int xfer_size, char *local_data) {
    int write_bytes = 0;

    write_bytes = write(sockfd, local_data, xfer_size);
    assert(write_bytes == xfer_size);

    // FIXME: hard code that always returns no error
    return 0;
}

// Sync data across a socket. The indicated local data will be sent to the
// remote. It will then wait for the remote to send its data back. It is
// assumned that the two sides are in sync and call this function in the proper
// order. Chaos will ensure if they are not. Also note this is a blocking
// function and will wait for the full data to be received from the remote.
static int sock_sync_data(int sockfd, int xfer_size, char *local_data, char *remote_data) {
    int read_bytes = 0;
    int write_bytes = 0;

    write_bytes = write(sockfd, local_data, xfer_size);
    assert(write_bytes == xfer_size);

    read_bytes = read(sockfd, remote_data, xfer_size);
    assert(read_bytes == xfer_size);

    INFO("SYNCHRONIZED!\n\n");

    // FIXME: hard code that always returns no error
    return 0;
}
// \end socket operation

static void print_config(struct config_t &config) {
    {
        INFO("Device name:          %s\n", config.dev_name.c_str());
        INFO("IB port:              %u\n", config.ib_port);
    }
    if (!config.server_name.empty()) {
        INFO("IP:                   %s\n", config.server_name.c_str());
    }
    {
        INFO("TCP port:             %u\n", config.tcp_port);
    }
    if (config.gid_idx >= 0) {
        INFO("GID index:            %u\n", config.gid_idx);
    }
}

static void print_usage(const char *progname) {
    printf("Usage:\n");
    printf("%s          start a server and wait for connection\n", progname);
    printf("%s <host>   connect to server at <host>\n\n", progname);
    printf("Options:\n");
    printf(
        "-p, --port <port>           listen on / connect to port <port> "
        "(default 20000)\n");
    printf(
        "-d, --ib-dev <dev>          use IB device <dev> (default first "
        "device found)\n");
    printf(
        "-i, --ib-port <port>        use port <port> of IB device (default "
        "1)\n");
    printf(
        "-g, --gid_idx <gid index>   gid index to be used in GRH (default "
        "not used)\n");
    printf("-h, --help                  this message\n");
}

static double BtoMB(uint64_t byte) {
    return static_cast<double>(byte) / 1024 / 1024;
}

// https://stackoverflow.com/a/11124118
// Returns the human-readable file size for an arbitrary, 64-bit file size
// The default format is "0.### XB", e.g. "4.2 KB" or "1.434 GB"
static std::string GetBytesReadable(std::size_t i) {
    // Determine the suffix and readable value
    std::string suffix;
    double readable;
    if (i >= 0x1000000000000000)  // Exabyte
    {
        suffix = "EB";
        readable = (i >> 50);
    } else if (i >= 0x4000000000000)  // Petabyte
    {
        suffix = "PB";
        readable = (i >> 40);
    } else if (i >= 0x10000000000)  // Terabyte
    {
        suffix = "TB";
        readable = (i >> 30);
    } else if (i >= 0x40000000)  // Gigabyte
    {
        suffix = "GB";
        readable = (i >> 20);
    } else if (i >= 0x100000)  // Megabyte
    {
        suffix = "MB";
        readable = (i >> 10);
    } else if (i >= 0x400)  // Kilobyte
    {
        suffix = "KB";
        readable = i;
    } else {
        return std::to_string(i) + " B";  // Byte
    }

    // Divide by 1024 to get fractional value
    readable = (readable / 1024);
    std::stringstream stream;
    stream << std::fixed << std::setprecision(2) << readable << suffix;

    // Return formatted number with suffix
    return stream.str();
}

#endif  // UTILS_H