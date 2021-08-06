#include "RDMAHandler.h"
#include <iostream>

uint32_t RDMAHandler::create_and_setup_region(config_t& config) {
    std::cout << "Handler creating a new ressource." << std::endl;
 	RDMARegion* region = new RDMARegion();
    // create resources before using them
	region->resources_create(config);

	// connect the QPs
	connect_qp(config, *region);

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