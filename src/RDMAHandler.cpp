#include "RDMAHandler.h"
#include "RDMACommunicator.h"
#include <iostream>
#include <thread>

void RDMAHandler::setupCommunicationBuffer(config_t& config) {
    std::cout << "Handler creating communication buffer." << std::endl;
 	RDMARegion* region = new RDMARegion();
    
    // create resources before using them
	region->resources_create(config);

	// connect the QPs
	connectQpTCP(config, *region);
    
    std::cout << "[RDMAHandler] Default region is: " << region << std::endl;
    communicationBuffer = region;
}

void RDMAHandler::create_and_setup_region( config_t* config, std::size_t bufferSize, uint64_t* newRegionId, bool* isReady ) {
    std::cout << "[RDMAHandler] Handler creating a new ressource" << std::endl;
 	
    RDMARegion* newRegion = new RDMARegion( bufferSize );
    // create resources before using them
	newRegion->resources_create(*config, false);

	// connect the QPs
    communicationBuffer->clearCompleteBuffer();

	sendRegionInfo( config, bufferSize, communicationBuffer, newRegion, rdma_create_region );
    
    while( communicationBuffer->receivePtr()[0] != rdma_receive_region ) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for( 100ms );
    }

    receiveRegionInfo( config, communicationBuffer, newRegion );
    auto id = registerRegion( newRegion );

    if ( newRegionId ) {
        *newRegionId = id;
    }

    communicationBuffer->clearCompleteBuffer();

    if ( isReady ) {
        *isReady = true;
    }
}

void RDMAHandler::sendRegionInfo( config_t* config, std::size_t bufferSize, RDMARegion* communicationRegion, RDMARegion* newRegion, rdma_handler_communication opcode ) {
    struct cm_con_data_t local_con_data;
    union ibv_gid my_gid;

    memset(&my_gid, 0, sizeof(my_gid));

    if (config->gid_idx >= 0) {
        CHECK(ibv_query_gid(newRegion->res.ib_ctx, config->ib_port, config->gid_idx, &my_gid));
    }

    local_con_data.addr = (uint64_t)newRegion->res.buf;
    local_con_data.rkey = newRegion->res.mr->rkey;
    local_con_data.qp_num = newRegion->res.qp->qp_num;
    local_con_data.lid = newRegion->res.port_attr.lid;
    memcpy(local_con_data.gid, &my_gid, 16);
    // INFO("\n Local LID      = 0x%x\n", newRegion->res.port_attr.lid);

    memcpy( communicationRegion->writePtr()+1, &local_con_data, sizeof( cm_con_data_t ) );
    memcpy( communicationRegion->writePtr()+1+sizeof( cm_con_data_t ), &bufferSize, sizeof( std::size_t ) );
    communicationRegion->post_send( 1+sizeof( local_con_data )+sizeof(std::size_t), IBV_WR_RDMA_WRITE, communicationRegion->maxWriteSize() );
    communicationRegion->poll_completion();

    communicationRegion->writePtr()[0] = opcode;
    communicationRegion->post_send( sizeof(char), IBV_WR_RDMA_WRITE, communicationRegion->maxWriteSize() );
    communicationRegion->poll_completion();
    // std::cout << "Sent data to remote machine:" << std::endl;
    // INFO("My address = 0x%" PRIx64 "\n",local_con_data.addr);
    // INFO("My rkey = 0x%x\n",            local_con_data.rkey);
    // INFO("My QP number = 0x%x\n",       local_con_data.qp_num);
    // INFO("My LID = 0x%x\n",             local_con_data.lid);
}

std::size_t RDMAHandler::receiveRegionSize( RDMARegion* communicationRegion ) const {
    std::size_t sz;
    memcpy( &sz, communicationRegion->receivePtr()+1+sizeof( cm_con_data_t ), sizeof(std::size_t) );
    return sz;
}

void RDMAHandler::receiveRegionInfo( config_t* config, RDMARegion* communicationRegion, RDMARegion* newRegion) {
    struct cm_con_data_t tmp_con_data;
    memcpy( &tmp_con_data, communicationRegion->receivePtr()+1, sizeof( cm_con_data_t ) );
    newRegion->resources_sync_local( config, tmp_con_data );

    // std::cout << "### Received RDMA Region ###" << std::endl;
    // INFO("Remote address = 0x%" PRIx64 "\n",tmp_con_data.addr);
    // INFO("Remote rkey = 0x%x\n",            tmp_con_data.rkey);
    // INFO("Remote QP number = 0x%x\n",       tmp_con_data.qp_num);
    // INFO("Remote LID = 0x%x\n",             tmp_con_data.lid);
    // std::cout << "############################" << std::endl;
}

uint64_t RDMAHandler::registerRegion( RDMARegion* region ) {
    regions.insert( {current_id, region} );   
    return current_id++;
}

void RDMAHandler::removeRegion( RDMARegion* region ) {
    for ( auto it = regions.begin(); it != regions.end(); ) {
        if ( it->second == region ) {
            std::cout << "[RDMAHandler] Removing Region [" << it->first << "] " << it->second << std::endl;
            it = regions.erase( it );
            delete region;
        } else {
            it++;
        }
    }
}

void RDMAHandler::printRegions() const {
    for ( auto it = regions.begin(); it != regions.end(); ++it ) {
        std::cout << "[RDMAHandler] Region [" << (*it).first << "]:" << std::endl;
        (*it).second->print();
        std::cout << std::endl;
    }
}

RDMARegion* RDMAHandler::getRegion( uint64_t id ) const {
    auto it = regions.find( id );
    if ( it != regions.end() ) {
        return it->second;
    } 
    return nullptr;
}


std::vector< RDMARegion* > RDMAHandler::getAllRegions() const {
    std::vector< RDMARegion* > rs;
    for ( auto it = regions.begin(); it != regions.end(); ++it ) {    
        rs.emplace_back( it->second );
    }
     return rs;
}

RDMARegion* RDMAHandler::selectRegion( bool withDefault ) {
    printRegions();
    if ( withDefault ) {
        std::cout << "Which region (\"d\" for default buffer)?" << std::endl;
    } else {
        std::cout << "Which region?" << std::endl;
    }
    std::string content;
    std::getline(std::cin, content);
    uint64_t rid;
    if (content != "d") {
        try {
            char* pEnd;
            rid = strtoull(content.c_str(), &pEnd, 10);
        } catch (...) {
            std::cout << "[Error] Couldn't convert number." << std::endl;
            return nullptr;
        }
        return getRegion( rid );
    } else {
        if ( !withDefault ) {
            return nullptr;
        }
        return communicationBuffer;
    }
};

// Connect the QP, then transition the server side to RTR, sender side to RTS.
void RDMAHandler::connectQpTCP( struct config_t& config, RDMARegion& region ) {
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    struct cm_con_data_t tmp_con_data;
    char temp_char;
    union ibv_gid my_gid;

    memset(&my_gid, 0, sizeof(my_gid));

    if (config.gid_idx >= 0) {
        CHECK(ibv_query_gid(region.res.ib_ctx, config.ib_port, config.gid_idx,
                            &my_gid));
    }

    // \begin exchange required info like buffer (addr & rkey) / qp_num / lid,
    // etc. exchange using TCP sockets info required to connect QPs
    local_con_data.addr = htonll((uintptr_t)region.res.buf);
    local_con_data.rkey = htonl(region.res.mr->rkey);
    local_con_data.qp_num = htonl(region.res.qp->qp_num);
    local_con_data.lid = htons(region.res.port_attr.lid);
    memcpy(local_con_data.gid, &my_gid, 16);

    INFO("\n Local LID      = 0x%x\n", region.res.port_attr.lid);

    sock_sync_data(region.res.sock, sizeof(struct cm_con_data_t),
                   (char *)&local_con_data, (char *)&tmp_con_data);

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
    
    // modify the QP to RTR
    region.modify_qp_to_rtr(config, region.res.qp, remote_con_data.qp_num, remote_con_data.lid,
                     remote_con_data.gid);

    // modify QP state to RTS
    region.modify_qp_to_rts(region.res.qp);

    // sync to make sure that both sides are in states that they can connect to
    // prevent packet lose
	char Q = 'Q';
    sock_sync_data(region.res.sock, 1, &Q, &temp_char);
    return;
}
