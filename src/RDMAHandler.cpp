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

void RDMAHandler::create_and_setup_region( config_t* config, uint64_t* newRegionId, bool* isReady ) {
    std::cout << "Handler creating a new ressource" << std::endl;
 	
    RDMARegion* newRegion = new RDMARegion();
    // create resources before using them
	newRegion->resources_create(*config, false);

	// connect the QPs
	sendRegionInfo( config, communicationBuffer, newRegion, rdma_create_region );
    
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

void RDMAHandler::sendRegionInfo( config_t* config, RDMARegion* communicationRegion, RDMARegion* newRegion, rdma_handler_communication opcode ) {
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
    INFO("\n Local LID      = 0x%x\n", newRegion->res.port_attr.lid);

    memcpy( communicationRegion->writePtr()+1, &local_con_data, sizeof( cm_con_data_t ) );
    post_send(&communicationRegion->res, sizeof( local_con_data ), IBV_WR_RDMA_WRITE, BUFF_SIZE/2 );
    poll_completion(&communicationRegion->res);

    communicationRegion->writePtr()[0] = opcode;
    post_send(&communicationRegion->res, sizeof(char), IBV_WR_RDMA_WRITE, BUFF_SIZE/2 );
    poll_completion(&communicationRegion->res);
    std::cout << "Sent data to remote machine:" << std::endl;
    INFO("My address = 0x%" PRIx64 "\n",local_con_data.addr);
    INFO("My rkey = 0x%x\n",            local_con_data.rkey);
    INFO("My QP number = 0x%x\n",       local_con_data.qp_num);
    INFO("My LID = 0x%x\n",             local_con_data.lid);
}

void RDMAHandler::receiveRegionInfo( config_t* config, RDMARegion* communicationRegion, RDMARegion* newRegion) {
    struct cm_con_data_t tmp_con_data;
    memcpy( &tmp_con_data, communicationRegion->receivePtr()+1, sizeof( cm_con_data_t ) );
    newRegion->resources_sync_local( config, tmp_con_data );

    std::cout << "### Received RDMA Region ###" << std::endl;
    INFO("Remote address = 0x%" PRIx64 "\n",tmp_con_data.addr);
    INFO("Remote rkey = 0x%x\n",            tmp_con_data.rkey);
    INFO("Remote QP number = 0x%x\n",       tmp_con_data.qp_num);
    INFO("Remote LID = 0x%x\n",             tmp_con_data.lid);
    std::cout << "############################" << std::endl;
}

uint64_t RDMAHandler::registerRegion( RDMARegion* region ) {
    regions.insert( {current_id, region} );   
    return current_id++;
}

void RDMAHandler::removeRegion( RDMARegion* region ) {
    for ( auto it = regions.begin(); it != regions.end(); ) {
        if ( it->second == region ) {
            std::cout << "Removing Region [" << it->first << "] " << it->second << std::endl;
            it = regions.erase( it );
            delete region;
        } else {
            it++;
        }
    }
}

void RDMAHandler::printRegions() const {
    for ( auto it = regions.begin(); it != regions.end(); ++it ) {
        std::cout << "Region [" << (*it).first << "]:" << std::endl;
        (*it).second->print();
        std::cout << std::endl;
    }
}

RDMARegion* RDMAHandler::getRegion( uint32_t id ) const {
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
