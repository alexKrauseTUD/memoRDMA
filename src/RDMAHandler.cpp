#include "RDMAHandler.h"
#include <iostream>
#include <thread>

void RDMAHandler::setupCommunicationBuffer(config_t& config) {
    std::cout << "Handler creating communication buffer." << std::endl;
 	RDMARegion* region = new RDMARegion();
    // create resources before using them
	region->resources_create(config);

	// connect the QPs
	connect_qp_tcp(config, *region);
    std::cout << "Region is: " << region << std::endl;
    communicationBuffer = region;
}

void RDMAHandler::create_and_setup_region( config_t* config, bool* isReady ) {
    std::cout << "Handler creating a new ressource" << std::endl;
 	
    RDMARegion* region = new RDMARegion();
    // create resources before using them
	region->resources_create(*config, false);

	// connect the QPs
	sendRegionInfo( config, region, rdma_create_region );
    
    while( communicationBuffer->receivePtr()[0] != rdma_receive_region ) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for( 100ms );
    }

    receiveRegionInfo( config, region );
    registerRegion( region );
    communicationBuffer->clearReadCode();

    if ( isReady ) {
        *isReady = true;
    }
    // return current_id++;
}

RDMARegion* RDMAHandler::getRegion( uint32_t id ) {
    auto it = regions.find( id );
    if ( it != regions.end() ) {
        return it->second;
    } 
    return nullptr;
}

void RDMAHandler::sendRegionInfo( config_t* config, RDMARegion* region, rdma_handler_communication opcode ) {
    struct cm_con_data_t local_con_data;
    union ibv_gid my_gid;

    memset(&my_gid, 0, sizeof(my_gid));

    if (config->gid_idx >= 0) {
        CHECK(ibv_query_gid(region->res.ib_ctx, config->ib_port, config->gid_idx, &my_gid));
    }

    local_con_data.addr = (uint64_t)region->res.buf;
    local_con_data.rkey = region->res.mr->rkey;
    local_con_data.qp_num = region->res.qp->qp_num;
    local_con_data.lid = region->res.port_attr.lid;
    memcpy(local_con_data.gid, &my_gid, 16);
    INFO("\n Local LID      = 0x%x\n", region->res.port_attr.lid);

    memcpy( communicationBuffer->writePtr()+1, &local_con_data, sizeof( cm_con_data_t ) );
    post_send(&communicationBuffer->res, sizeof( local_con_data ), IBV_WR_RDMA_WRITE, BUFF_SIZE/2 );
    poll_completion(&communicationBuffer->res);

    communicationBuffer->writePtr()[0] = opcode;
    post_send(&communicationBuffer->res, sizeof(char), IBV_WR_RDMA_WRITE, BUFF_SIZE/2 );
    poll_completion(&communicationBuffer->res);
    std::cout << "Sent data to remote machine:" << std::endl;
    INFO("My address = 0x%" PRIx64 "\n",local_con_data.addr);
    INFO("My rkey = 0x%x\n",            local_con_data.rkey);
    INFO("My QP number = 0x%x\n",       local_con_data.qp_num);
    INFO("My LID = 0x%x\n",             local_con_data.lid);
}

void RDMAHandler::receiveRegionInfo( config_t* config, RDMARegion* region) {
    struct cm_con_data_t tmp_con_data;
    memcpy( &tmp_con_data, communicationBuffer->receivePtr()+1, sizeof( cm_con_data_t ) );
    region->resources_sync_local( config, tmp_con_data );

    std::cout << "### Received RDMA Region ###" << std::endl;
    INFO("Remote address = 0x%" PRIx64 "\n",tmp_con_data.addr);
    INFO("Remote rkey = 0x%x\n",            tmp_con_data.rkey);
    INFO("Remote QP number = 0x%x\n",       tmp_con_data.qp_num);
    INFO("Remote LID = 0x%x\n",             tmp_con_data.lid);
    std::cout << "############################" << std::endl;
}

void RDMAHandler::registerRegion( RDMARegion* region ) {
    regions.insert( {current_id++, region} );   
}

void RDMAHandler::printRegions() const {
    for ( auto it = regions.begin(); it != regions.end(); ++it ) {
        std::cout << "Region [" << (*it).first << "]:" << std::endl;
        (*it).second->print();
        std::cout << std::endl;
    }
}