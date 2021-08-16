#include <iostream>
#include <stdlib.h>
#include <stdint.h>
#include "DataProvider.h"

DataProvider::DataProvider() : 
    currSize(0),
    data( nullptr )
{
}

DataProvider::~DataProvider() {
    if ( data ) {
        free( data );
    }
}

void DataProvider::generateDummyData( uint64_t count ) {
    if ( data ) {
        free( data );
    }
    currSize = count;
    data = (uint64_t*) malloc( count * sizeof(uint64_t) );
    for ( size_t i = 0; i < count; ++i ) {
        data[i] = i; // sequential data
        // data[i] = xorshift64(); // random data
    }
}