#include "RDMAHandler.h"
#include <iostream>

uint32_t RDMAHandler::setupCommunicationBuffer(config_t& config) {
    std::cout << "Handler creating a new ressource." << std::endl;
 	RDMARegion* region = new RDMARegion();
    // create resources before using them
	region->resources_create(config);

	// connect the QPs
	connect_qp_tcp(config, *region);

    regions.insert( {current_id, region} );
    return current_id++;
}


uint32_t RDMAHandler::create_and_setup_region(config_t& config) {
    std::cout << "Handler creating a new ressource." << std::endl;
 	
     RDMARegion* region = new RDMARegion();
    // create resources before using them
	region->resources_create(config);

	// connect the QPs
	connect_qp_rdma(config, *region);

    regions.insert( {current_id, region} );
    return current_id++;
}

RDMARegion* RDMAHandler::getRegion( uint32_t id ) {
    auto it = regions.find( id );
    if ( it != regions.end() ) {
        return it->second;
    } 
    return nullptr;
}

void RDMAHandler::connect_qp_rdma(struct config_t& config, RDMARegion& region) {
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    struct cm_con_data_t tmp_con_data;
    union ibv_gid my_gid;

    memset(&my_gid, 0, sizeof(my_gid));

    if (config.gid_idx >= 0) {
        CHECK(ibv_query_gid(region.res.ib_ctx, config.ib_port, config.gid_idx, &my_gid));
    }

    // \begin exchange required info like buffer (addr & rkey) / qp_num / lid,
    // etc. exchange using TCP sockets info required to connect QPs
    local_con_data.addr = htonll((uintptr_t)region.res.buf);
    local_con_data.rkey = htonl(region.res.mr->rkey);
    local_con_data.qp_num = htonl(region.res.qp->qp_num);
    local_con_data.lid = htons(region.res.port_attr.lid);
    memcpy(local_con_data.gid, &my_gid, 16);

    INFO("\n Local LID      = 0x%x\n", region.res.port_attr.lid);

    /* Step 1: Server creates data and sends to Client */

    /* Step 2: Client creates local resources and sends back to server. */
    sock_sync_data(region.res.sock, sizeof(struct cm_con_data_t), (char *)&local_con_data, (char *)&tmp_con_data);

    remote_con_data.addr = ntohll(tmp_con_data.addr);
    remote_con_data.rkey = ntohl(tmp_con_data.rkey);
    remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
    remote_con_data.lid = ntohs(tmp_con_data.lid);
    memcpy(remote_con_data.gid, tmp_con_data.gid, 16);

    // save the remote side attributes, we will need it for the post SR
    region.res.remote_props = remote_con_data;
    // \end exchange required info

    INFO("Remote address = 0x%" PRIx64 "\n", remote_con_data.addr);
    INFO("Remote rkey = 0x%x\n", remote_con_data.rkey);
    INFO("Remote QP number = 0x%x\n", remote_con_data.qp_num);
    INFO("Remote LID = 0x%x\n", remote_con_data.lid);

    if (config.gid_idx >= 0) {
        uint8_t *p = remote_con_data.gid;
        int i;
        printf("Remote GID = ");
        for (i = 0; i < 15; i++)
            printf("%02x:", p[i]);
        printf("%02x\n", p[15]);
    }

    // modify the QP to init
    region.modify_qp_to_init(config, region.res.qp);

    // let the client post RR to be prepared for incoming messages
    if (config.server_name) {
        post_receive(&region.res);
    }
    
    // modify the QP to RTR
    region.modify_qp_to_rtr(config, region.res.qp, remote_con_data.qp_num, remote_con_data.lid,
                     remote_con_data.gid);

    // modify QP state to RTS
    region.modify_qp_to_rts(region.res.qp);
}