#include <iostream>
#include <stdlib.h>
#include <stdint.h>
#include "DataProvider.h"

DataProvider::DataProvider() : 
    currSize(0),
    data( nullptr )
{
}

void DataProvider::generateDummyData( uint64_t size ) {
    if ( data ) {
        delete data;
    }
    currSize = size;
    data = (uint64_t*) malloc( size * sizeof(uint64_t) );
    for ( size_t i = 0; i < size; ++i ) {
        data[i] = i; // sequential data
        // data[i] = xorshift64(); // random data
    }
}